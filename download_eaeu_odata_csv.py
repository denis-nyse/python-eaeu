import argparse
import ast
import csv
import json
import logging
import os
import random
import time
from datetime import datetime, timedelta, timezone

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://opendata.eaeunion.org/odata/conformityDocDetailsType"
VALID_COUNTRY_CODES = ["AM", "BY", "KG", "KZ", "RU"]
COUNTRY_FIELD = "unifiedCountryCode.value"
COUNTRY_NAMES_RU = {
    "AM": "Армения",
    "BY": "Беларусь",
    "KG": "Кыргызстан",
    "KZ": "Казахстан",
    "RU": "Россия",
}
OUTPUT_COLUMNS = [
    "Регистрационный номер документа",
    "Страна",
    "Вид документа",
    "Срок действия",
    "Заявитель",
    "Изготовитель",
    "Технический регламент",
    "Наименование органа по оценке соответствия",
    "Статус действия",
]
DEFAULT_REQUEST_TIMEOUT_SECONDS = 60
DEFAULT_MAX_REQUEST_RETRIES = 6
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
RETRY_BACKOFF_SECONDS = 1.0
MAX_CONSECUTIVE_BATCH_ERRORS = 8
LOG_FORMAT = "%(asctime)s | %(levelname)s | %(message)s"
LOGGER = logging.getLogger("eaeu_odata_export")
DEFAULT_STATE_FILE = ".eaeu_export_state.json"


class CsvPartWriter:
    def __init__(self, filename: str, max_rows_per_file: int, fieldnames: list[str]):
        if max_rows_per_file <= 0:
            raise ValueError("--max-rows-per-file должен быть больше 0.")
        self.filename = filename
        self.max_rows_per_file = max_rows_per_file
        self.fieldnames = fieldnames

        self._file = None
        self._writer = None
        self._rows_in_part = 0
        self._part_index = 0
        self.total_rows = 0
        self.files_created: list[str] = []

    def _split_name(self, part_index: int) -> str:
        if "." in self.filename:
            base, ext = self.filename.rsplit(".", 1)
            ext = f".{ext}"
        else:
            base, ext = self.filename, ".csv"

        if part_index == 1:
            return self.filename
        return f"{base}_part{part_index:03d}{ext}"

    def _open_next_file(self) -> None:
        self.close_current()
        self._part_index += 1
        path = self._split_name(self._part_index)
        self._file = open(path, "w", newline="", encoding="utf-8-sig")
        self._writer = csv.DictWriter(self._file, fieldnames=self.fieldnames, delimiter=";")
        self._writer.writeheader()
        self._rows_in_part = 0
        self.files_created.append(path)
        print(f"Открыт файл: {path}")

    def close_current(self) -> None:
        if self._file is not None:
            self._file.close()
        self._file = None
        self._writer = None

    def write_row(self, row: dict[str, str]) -> None:
        if self._writer is None:
            self._open_next_file()

        if self._rows_in_part >= self.max_rows_per_file:
            self._open_next_file()

        self._writer.writerow(row)
        self._rows_in_part += 1
        self.total_rows += 1

    def write_rows(self, rows: list[dict[str, str]]) -> None:
        for row in rows:
            self.write_row(row)

    def close(self) -> None:
        self.close_current()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Потоковая выгрузка данных ЕАЭС через ODATA в CSV.")
    parser.add_argument(
        "--countries",
        type=str,
        default="",
        help="Коды стран через запятую (например: RU,BY), ALL или ASK. Если не указано, будет интерактивный выбор.",
    )
    parser.add_argument(
        "--updated-from",
        type=str,
        default=None,
        help=(
            "Фильтр обновлений по дате/времени: "
            "resourceItemStatusDetails/updateDateTime ge <timestamp>, "
            "например 24.06.2024, 2024-06-24 или 2024-06-24T00:00:00.00Z."
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=30,
        help="Размер пачки одного запроса (по умолчанию 30, максимум 10000).",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.0,
        help="Базовая пауза между запросами в секундах (по умолчанию 0).",
    )
    parser.add_argument(
        "--sleep-jitter-min",
        type=float,
        default=1.0,
        help="Минимальная случайная пауза между запросами (по умолчанию 1.0).",
    )
    parser.add_argument(
        "--sleep-jitter-max",
        type=float,
        default=3.0,
        help="Максимальная случайная пауза между запросами (по умолчанию 3.0).",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="",
        help="Имя выходного CSV. Если не указано, имя будет сгенерировано автоматически.",
    )
    parser.add_argument(
        "--max-rows-per-file",
        type=int,
        default=10000,
        help="Максимум строк в одном CSV файле (по умолчанию 10000).",
    )
    parser.add_argument(
        "--request-timeout",
        type=float,
        default=DEFAULT_REQUEST_TIMEOUT_SECONDS,
        help=f"Таймаут одного HTTP-запроса в секундах (по умолчанию {DEFAULT_REQUEST_TIMEOUT_SECONDS}).",
    )
    parser.add_argument(
        "--request-retries",
        type=int,
        default=DEFAULT_MAX_REQUEST_RETRIES,
        help=(
            "Количество ретраев на уровне HTTP-клиента только для connect-ошибок "
            f"(по умолчанию {DEFAULT_MAX_REQUEST_RETRIES}; read/status-ретраи отключены)."
        ),
    )
    parser.add_argument(
        "--slice-by",
        type=str,
        choices=["none", "year", "month"],
        default="year",
        help="Разбивка выгрузки на интервалы: none/year/month (по умолчанию year).",
    )
    parser.add_argument(
        "--slice-date-field",
        type=str,
        choices=["docStartDate", "docCreationDate"],
        default="docStartDate",
        help="Поле даты для интервальной разбивки (по умолчанию docStartDate).",
    )
    parser.add_argument(
        "--slice-start",
        type=str,
        default="2015-01-01",
        help="Начало диапазона для разбивки, формат YYYY-MM-DD (по умолчанию 2015-01-01).",
    )
    parser.add_argument(
        "--slice-end",
        type=str,
        default="",
        help="Конец диапазона для разбивки, формат YYYY-MM-DD. По умолчанию текущая дата UTC.",
    )
    parser.add_argument(
        "--user-agent",
        type=str,
        default=DEFAULT_USER_AGENT,
        help="Заголовок User-Agent для HTTP-запросов.",
    )
    parser.add_argument(
        "--date-filter-mode",
        type=str,
        choices=["auto", "server", "client"],
        default="auto",
        help=(
            "Режим фильтра updated-from: "
            "server — фильтр на стороне API, "
            "client — фильтр локально после загрузки, "
            "auto — сначала server, при 504 автоматически client (по умолчанию)."
        ),
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Продолжить выгрузку с последнего сохраненного skip из state-файла.",
    )
    parser.add_argument(
        "--state-file",
        type=str,
        default=DEFAULT_STATE_FILE,
        help=f"Файл состояния для resume (по умолчанию {DEFAULT_STATE_FILE}).",
    )
    parser.add_argument(
        "--reset-state",
        action="store_true",
        help="Сбросить state-файл перед стартом новой выгрузки.",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Уровень логирования (по умолчанию INFO).",
    )
    parser.add_argument(
        "--log-file",
        type=str,
        default="",
        help="Путь к файлу логов. Если не указан, логи идут только в консоль.",
    )
    return parser.parse_args()


def configure_logging(log_level: str, log_file: str) -> None:
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    if log_file:
        handlers.append(logging.FileHandler(log_file, mode="a", encoding="utf-8"))

    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format=LOG_FORMAT,
        handlers=handlers,
    )


def load_state(path: str) -> dict:
    if not os.path.exists(path):
        return {"countries": {}}
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        return {"countries": {}}
    if "countries" not in data or not isinstance(data["countries"], dict):
        data["countries"] = {}
    return data


def save_state(path: str, state: dict) -> None:
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, path)


def ask_countries_interactive() -> list[str]:
    print("Выберите страну для выгрузки:")
    print("1. ALL (все страны ЕАЭС)")
    print("2. RU")
    print("3. BY")
    print("4. KZ")
    print("5. KG")
    print("6. AM")
    choice = input("Введите номер (1-6): ").strip()
    mapping = {
        "1": VALID_COUNTRY_CODES,
        "2": ["RU"],
        "3": ["BY"],
        "4": ["KZ"],
        "5": ["KG"],
        "6": ["AM"],
    }
    if choice not in mapping:
        raise ValueError("Неверный выбор. Используйте номер от 1 до 6.")
    return mapping[choice]


def ask_updated_from_interactive() -> str | None:
    print("Фильтр по дате обновления (рекомендуется для актуализации):")
    print("1. Без фильтра по дате обновления")
    print("2. Выгружать обновленные с даты/времени")
    choice = input("Введите номер (1-2): ").strip()
    if choice == "1":
        return None
    if choice != "2":
        raise ValueError("Неверный выбор. Используйте 1 или 2.")

    ts_raw = input(
        "Введите дату/время (например 24.06.2024, 2024-06-24, 2024-06 или 2024): "
    ).strip()
    if not ts_raw:
        raise ValueError("Дата/время не указаны.")
    return normalize_utc_timestamp(ts_raw)


def normalize_countries(raw_value: str) -> list[str]:
    value = raw_value.strip().upper()
    if value == "ALL":
        return VALID_COUNTRY_CODES[:]

    countries = [item.strip().upper() for item in value.split(",") if item.strip()]
    if not countries:
        raise ValueError("Список стран пуст.")

    unknown = [code for code in countries if code not in VALID_COUNTRY_CODES]
    if unknown:
        raise ValueError(
            f"Неверные коды стран: {', '.join(unknown)}. "
            f"Разрешены: {', '.join(VALID_COUNTRY_CODES)}."
        )
    return list(dict.fromkeys(countries))


def normalize_utc_timestamp(value: str) -> str:
    text = value.strip()
    if not text:
        raise ValueError("Пустая дата/время.")

    # Формат пользователя: гггг или гггг-мм
    if len(text) == 4 and text.isdigit():
        text = f"{text}-01-01T00:00:00.00Z"
    elif len(text) == 7 and text[4] == "-" and text[:4].isdigit() and text[5:7].isdigit():
        year = int(text[:4])
        month = int(text[5:7])
        if not 1 <= month <= 12:
            raise ValueError("Неверный месяц в формате гггг-мм.")
        text = f"{year:04d}-{month:02d}-01T00:00:00.00Z"

    # Формат пользователя: дд.мм.гггг
    if len(text) == 10 and text[2] == "." and text[5] == ".":
        try:
            dt = datetime.strptime(text, "%d.%m.%Y")
            text = dt.strftime("%Y-%m-%dT00:00:00.00Z")
        except ValueError as exc:
            raise ValueError(
                "Неверная дата в формате дд.мм.гггг."
            ) from exc

    # Если ввели только дату, приводим к началу дня в UTC.
    if len(text) == 10 and text.count("-") == 2:
        text = f"{text}T00:00:00.00Z"

    probe = text.replace("Z", "+00:00")
    try:
        datetime.fromisoformat(probe)
    except ValueError as exc:
        raise ValueError(
            "Неверный формат даты/времени. Используйте ISO-8601, "
            "например 2024-06-24T00:00:00.00Z, 2024-06-24, 2024-06 или 2024."
        ) from exc

    if text.endswith("+00:00"):
        text = text[:-6] + "Z"
    return text


def parse_iso_datetime_utc(value: str) -> datetime | None:
    text = value.strip() if isinstance(value, str) else ""
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def parse_yyyy_mm_dd(value: str, field_name: str) -> datetime:
    try:
        parsed = datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(f"{field_name} должен быть в формате YYYY-MM-DD.") from exc
    return parsed.replace(tzinfo=timezone.utc)


def iter_time_slices(slice_by: str, start_utc: datetime, end_utc: datetime) -> list[tuple[str, str, str]]:
    if slice_by == "none":
        return [("all", start_utc.strftime("%Y-%m-%d"), end_utc.strftime("%Y-%m-%d"))]

    slices: list[tuple[str, str, str]] = []
    cursor = datetime(start_utc.year, start_utc.month, 1, tzinfo=timezone.utc)
    while cursor <= end_utc:
        if slice_by == "year":
            part_start = datetime(cursor.year, 1, 1, tzinfo=timezone.utc)
            part_end = datetime(cursor.year, 12, 31, tzinfo=timezone.utc)
            label = f"{cursor.year}"
            cursor = datetime(cursor.year + 1, 1, 1, tzinfo=timezone.utc)
        else:
            part_start = datetime(cursor.year, cursor.month, 1, tzinfo=timezone.utc)
            if cursor.month == 12:
                next_month = datetime(cursor.year + 1, 1, 1, tzinfo=timezone.utc)
            else:
                next_month = datetime(cursor.year, cursor.month + 1, 1, tzinfo=timezone.utc)
            part_end = next_month - timedelta(days=1)
            label = f"{cursor.year:04d}-{cursor.month:02d}"
            cursor = next_month

        if part_end < start_utc or part_start > end_utc:
            continue

        clipped_start = max(part_start, start_utc)
        clipped_end = min(part_end, end_utc)
        slices.append(
            (
                label,
                clipped_start.strftime("%Y-%m-%d"),
                clipped_end.strftime("%Y-%m-%d"),
            )
        )
    return slices


def maybe_sleep(base_sleep: float, jitter_min: float, jitter_max: float) -> None:
    pause = max(0.0, base_sleep)
    if jitter_max > 0:
        pause += random.uniform(jitter_min, jitter_max)
    if pause > 0:
        time.sleep(pause)


def create_http_session(max_request_retries: int, user_agent: str) -> requests.Session:
    retry = Retry(
        total=max_request_retries,
        connect=max_request_retries,
        read=0,
        status=0,
        backoff_factor=RETRY_BACKOFF_SECONDS,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset({"GET"}),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.headers.update({"User-Agent": user_agent})
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def output_name(countries: list[str], updated_from: str | None, explicit_name: str) -> str:
    if explicit_name:
        return explicit_name
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix = "ALL" if set(countries) == set(VALID_COUNTRY_CODES) else "_".join(countries)
    if updated_from:
        return f"export_odata_{suffix}_updated_{stamp}.csv"
    return f"export_odata_{suffix}_{stamp}.csv"


def odata_extract_records(payload: object) -> list[object]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        data = (
            payload.get("value")
            or payload.get("Value")
            or payload.get("result")
            or payload.get("Result")
            or payload.get("data")
            or payload.get("items")
            or []
        )
        return data if isinstance(data, list) else [data]
    return [payload]


def parse_structured_value(value):
    if value is None:
        return ""
    if isinstance(value, (list, dict)):
        return value
    if not isinstance(value, str):
        return value

    text = value.strip()
    if not text or text in {"[]", "{}", "None", "nan", "NaN", "null"}:
        return ""

    if (text.startswith("[") and text.endswith("]")) or (
        text.startswith("{") and text.endswith("}")
    ):
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass
        try:
            return ast.literal_eval(text)
        except (ValueError, SyntaxError):
            return value

    return value


def flatten_for_humans(value) -> str:
    if isinstance(value, list):
        if not value:
            return ""
        items = [flatten_for_humans(item) for item in value]
        items = [item for item in items if item]
        return " | ".join(items)

    if isinstance(value, dict):
        parts = []
        for key, raw in value.items():
            flat = flatten_for_humans(raw)
            if flat:
                parts.append(f"{key}: {flat}")
        return "; ".join(parts)

    text = str(value).strip() if value is not None else ""
    if text in {"None", "nan", "NaN", "null", "[]", "{}"}:
        return ""
    return text


def get_nested(obj: dict, path: str, default=""):
    current = obj
    for part in path.split("."):
        if isinstance(current, dict) and part in current:
            current = current[part]
        else:
            return default
    return current


def get_date_value(record: dict, field_name: str):
    direct = record.get(field_name, "")
    if isinstance(direct, dict):
        nested = direct.get("$date", "")
        if nested:
            return nested
    if direct:
        return direct

    dotted = record.get(f"{field_name}.$date", "")
    if dotted:
        return dotted

    nested_by_path = get_nested(record, f"{field_name}.$date", "")
    if nested_by_path:
        return nested_by_path

    return ""


def get_update_datetime_value(record: dict) -> str:
    candidates = [
        get_date_value(record, "resourceItemStatusDetails/updateDateTime"),
        get_date_value(record, "resourceItemStatusDetails.updateDateTime"),
        get_nested(record, "resourceItemStatusDetails.updateDateTime", ""),
        get_nested(record, "resourceItemStatusDetails.updateDateTime.$date", ""),
        record.get("resourceItemStatusDetails/updateDateTime", ""),
    ]
    for candidate in candidates:
        if isinstance(candidate, dict):
            nested = candidate.get("$date", "")
            if nested:
                return str(nested)
        if candidate:
            return str(candidate)
    return ""


def record_matches_updated_from(record: dict, updated_from_dt: datetime | None) -> bool:
    if updated_from_dt is None:
        return True
    update_text = get_update_datetime_value(record)
    update_dt = parse_iso_datetime_utc(update_text)
    if update_dt is None:
        return False
    return update_dt >= updated_from_dt


def build_slice_clauses(date_field: str, start_date: str, end_date: str) -> list[str]:
    start_ts = f"{start_date}T00:00:00.00Z"
    end_ts = f"{end_date}T23:59:59.99Z"
    return [
        f"{date_field} ge {start_ts}",
        f"{date_field} le {end_ts}",
    ]


def normalize_record(item: object, country_code: str) -> dict:
    if isinstance(item, dict):
        row = item.copy()
    elif isinstance(item, str):
        stripped = item.strip()
        parsed = None
        if stripped:
            try:
                parsed = json.loads(stripped)
            except json.JSONDecodeError:
                parsed = None
        if isinstance(parsed, dict):
            row = parsed
        else:
            row = {"_raw_value": item, "_raw_type": "str"}
    elif isinstance(item, list):
        row = {"_raw_value": json.dumps(item, ensure_ascii=False), "_raw_type": "list"}
    else:
        row = {"_raw_value": item, "_raw_type": type(item).__name__}

    if COUNTRY_FIELD not in row or not row.get(COUNTRY_FIELD):
        row[COUNTRY_FIELD] = country_code
    return row


def to_ddmmyyyy(value) -> str:
    text = flatten_for_humans(value).strip()
    if not text:
        return ""
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return dt.strftime("%d.%m.%Y")
    except ValueError:
        return ""


def extract_from_structured(value, key: str) -> str:
    parsed = parse_structured_value(value)
    if isinstance(parsed, dict):
        return flatten_for_humans(parsed.get(key, ""))
    if isinstance(parsed, list):
        values = []
        for item in parsed:
            if isinstance(item, dict) and key in item:
                text = flatten_for_humans(item.get(key, ""))
                if text:
                    values.append(text)
        if values:
            return " | ".join(list(dict.fromkeys(values)))
    return ""


def status_from_record(record: dict) -> str:
    end_date_raw = get_date_value(record, "docValidityDate")
    status_code = flatten_for_humans(get_nested(record, "docStatusDetails.docStatusCode", "")).strip()
    note_text = flatten_for_humans(get_nested(record, "docStatusDetails.noteText", "")).strip().lower()

    end_text = flatten_for_humans(end_date_raw).strip()
    if end_text:
        try:
            end_dt = datetime.fromisoformat(end_text.replace("Z", "+00:00"))
            now = datetime.now(timezone.utc)
            if end_dt.tzinfo is None:
                end_dt = end_dt.replace(tzinfo=timezone.utc)
            return "действует" if end_dt >= now else "прекращен"
        except ValueError:
            pass

    if "прекращ" in note_text:
        return "прекращен"
    if status_code in {"09", "10"}:
        return "прекращен"
    if status_code:
        return "действует"
    return ""


def record_to_selected_row(record: dict) -> dict[str, str]:
    country_code = flatten_for_humans(record.get(COUNTRY_FIELD, "")).strip()

    applicant = flatten_for_humans(get_nested(record, "applicantDetails.businessEntityName", ""))
    manufacturer = extract_from_structured(
        get_nested(record, "technicalRegulationObjectDetails.manufacturerDetails", ""),
        "businessEntityName",
    )
    if not manufacturer:
        manufacturer = applicant

    start = to_ddmmyyyy(get_date_value(record, "docStartDate"))
    end = to_ddmmyyyy(get_date_value(record, "docValidityDate"))
    term = f"{start} - {end}" if start and end else (start or end)

    return {
        "Регистрационный номер документа": flatten_for_humans(record.get("docId", "")),
        "Страна": COUNTRY_NAMES_RU.get(country_code, country_code),
        "Вид документа": flatten_for_humans(record.get("conformityDocKindName", "")),
        "Срок действия": term,
        "Заявитель": applicant,
        "Изготовитель": manufacturer,
        "Технический регламент": flatten_for_humans(parse_structured_value(record.get("technicalRegulationId", ""))),
        "Наименование органа по оценке соответствия": flatten_for_humans(
            get_nested(record, "conformityAuthorityV2Details.businessEntityName", "")
        ),
        "Статус действия": status_from_record(record),
    }


def build_odata_filter(
    country_code: str,
    updated_from: str | None,
    apply_server_updated_filter: bool,
    extra_clauses: list[str] | None = None,
) -> str:
    clauses = [f"unifiedCountryCode/value eq '{country_code}'"]
    if updated_from and apply_server_updated_filter:
        clauses.append(f"resourceItemStatusDetails/updateDateTime ge {updated_from}")
    if extra_clauses:
        clauses.extend(extra_clauses)
    return " and ".join(clauses)


def fetch_batch(
    session: requests.Session,
    country_code: str,
    skip: int,
    top: int,
    updated_from: str | None,
    apply_server_updated_filter: bool,
    extra_clauses: list[str] | None,
    request_timeout: float,
) -> list[object]:
    params = {
        "$top": top,
        "$skip": skip,
        "$filter": build_odata_filter(
            country_code,
            updated_from,
            apply_server_updated_filter,
            extra_clauses,
        ),
        "$orderby": "docCreationDate desc",
    }

    started = time.monotonic()
    LOGGER.debug(
        "HTTP request start: country=%s skip=%s top=%s updated_from=%s server_filter=%s",
        country_code,
        skip,
        top,
        updated_from,
        apply_server_updated_filter,
    )
    response = session.get(BASE_URL, params=params, timeout=request_timeout)
    elapsed = time.monotonic() - started
    LOGGER.debug(
        "HTTP response: status=%s elapsed=%.2fs url=%s",
        response.status_code,
        elapsed,
        response.url,
    )
    response.raise_for_status()
    return odata_extract_records(response.json())


def stream_country(
    session: requests.Session,
    country_code: str,
    limit: int,
    sleep_seconds: float,
    updated_from: str | None,
    updated_from_dt: datetime | None,
    date_filter_mode: str,
    start_skip: int,
    extra_clauses: list[str] | None,
    slice_label: str,
    request_timeout: float,
    jitter_min: float,
    jitter_max: float,
    writer: CsvPartWriter,
    progress_callback,
) -> int:
    total_written = 0
    skip = start_skip
    batch_count = (start_skip // limit) + 1
    consecutive_errors = 0
    use_server_updated_filter = bool(updated_from) and date_filter_mode in {"server", "auto"}
    use_client_updated_filter = bool(updated_from) and date_filter_mode == "client"

    print(
        f"\nСтарт выгрузки страны {country_code} "
        f"(limit={limit}, обновлено с: {updated_from if updated_from else 'без фильтра'}, "
        f"skip={start_skip}, режим={date_filter_mode}, интервал={slice_label})"
    )

    while True:
        try:
            data = fetch_batch(
                session=session,
                country_code=country_code,
                skip=skip,
                top=limit,
                updated_from=updated_from,
                apply_server_updated_filter=use_server_updated_filter,
                extra_clauses=extra_clauses,
                request_timeout=request_timeout,
            )
            consecutive_errors = 0
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            if (
                status == 504
                and updated_from
                and date_filter_mode == "auto"
                and use_server_updated_filter
            ):
                use_server_updated_filter = False
                use_client_updated_filter = True
                LOGGER.warning(
                    "%s: API возвращает 504 на server updated-filter, переключаюсь на client-filter.",
                    country_code,
                )
                print(
                    f"{country_code}: API вернул 504 на серверный фильтр по дате. "
                    "Переключаюсь на локальный фильтр по updateDateTime."
                )
                maybe_sleep(max(2.0, sleep_seconds), jitter_min, jitter_max)
                continue
            if status is not None and 400 <= status < 500 and status != 429:
                body = (exc.response.text or "").strip()[:500] if exc.response is not None else ""
                details = f" HTTP {status}."
                if body:
                    details += f" Ответ API: {body}"
                raise RuntimeError(
                    f"{country_code}: остановка на skip={skip}.{details}"
                ) from exc
            consecutive_errors += 1
            LOGGER.warning(
                "%s: HTTP ошибка на skip=%s (status=%s), попытка %s/%s",
                country_code,
                skip,
                status,
                consecutive_errors,
                MAX_CONSECUTIVE_BATCH_ERRORS,
            )
            if consecutive_errors >= MAX_CONSECUTIVE_BATCH_ERRORS:
                raise RuntimeError(
                    f"{country_code}: слишком много HTTP-ошибок подряд "
                    f"({consecutive_errors}) на skip={skip}."
                ) from exc
            print(
                f"{country_code}: HTTP ошибка на skip={skip} (status={status}). "
                "Повторю через паузу."
            )
            maybe_sleep(max(2.0, sleep_seconds), jitter_min, jitter_max)
            continue
        except requests.RequestException as exc:
            consecutive_errors += 1
            if consecutive_errors >= MAX_CONSECUTIVE_BATCH_ERRORS:
                raise RuntimeError(
                    f"{country_code}: слишком много сетевых ошибок подряд "
                    f"({consecutive_errors}) на skip={skip}."
                ) from exc
            LOGGER.warning(
                "%s: сетевая ошибка на skip=%s, попытка %s/%s: %s",
                country_code,
                skip,
                consecutive_errors,
                MAX_CONSECUTIVE_BATCH_ERRORS,
                exc,
            )
            print(
                f"{country_code}: ошибка сети на skip={skip}: {exc}. "
                "Повторю через паузу."
            )
            maybe_sleep(max(2.0, sleep_seconds), jitter_min, jitter_max)
            continue

        if not data:
            print(f"{country_code}: данные закончились на skip={skip}.")
            progress_callback(country_code, skip, total_written, True, use_client_updated_filter)
            break

        rows = []
        for item in data:
            record = normalize_record(item, country_code)
            if use_client_updated_filter and not record_matches_updated_from(record, updated_from_dt):
                continue
            rows.append(record_to_selected_row(record))

        if rows:
            writer.write_rows(rows)
            total_written += len(rows)

        print(
            f"{country_code} | пачка #{batch_count}: получено {len(data)} записей, "
            f"записано {len(rows)} (skip={skip}, top={limit})"
        )
        next_skip = skip + len(data)
        progress_callback(country_code, next_skip, total_written, False, use_client_updated_filter)

        if len(data) < limit:
            print(f"{country_code}: достигнут конец доступных данных.")
            progress_callback(country_code, next_skip, total_written, True, use_client_updated_filter)
            break

        skip = next_skip
        batch_count += 1
        maybe_sleep(sleep_seconds, jitter_min, jitter_max)

    print(f"{country_code}: записано {total_written} строк.")
    return total_written


def main() -> None:
    args = parse_args()
    configure_logging(args.log_level, args.log_file)
    LOGGER.info("Запуск выгрузки ЕАЭС ODATA")
    LOGGER.info(
        (
            "Параметры: countries=%s updated_from=%s limit=%s sleep=%s jitter=[%s,%s] "
            "max_rows_per_file=%s request_timeout=%s request_retries=%s "
            "date_filter_mode=%s slice_by=%s slice_field=%s slice_start=%s slice_end=%s "
            "resume=%s state_file=%s"
        ),
        args.countries if args.countries else "ASK",
        args.updated_from if args.updated_from is not None else "ASK",
        args.limit,
        args.sleep,
        args.sleep_jitter_min,
        args.sleep_jitter_max,
        args.max_rows_per_file,
        args.request_timeout,
        args.request_retries,
        args.date_filter_mode,
        args.slice_by,
        args.slice_date_field,
        args.slice_start,
        args.slice_end if args.slice_end else "today",
        args.resume,
        args.state_file,
    )

    if args.limit <= 0:
        raise ValueError("--limit должен быть больше 0.")
    if args.limit > 10000:
        raise ValueError("--limit не должен быть больше 10000.")
    if args.request_timeout <= 0:
        raise ValueError("--request-timeout должен быть больше 0.")
    if args.request_retries < 0:
        raise ValueError("--request-retries не может быть отрицательным.")
    if args.sleep < 0:
        raise ValueError("--sleep не может быть отрицательным.")
    if args.sleep_jitter_min < 0 or args.sleep_jitter_max < 0:
        raise ValueError("--sleep-jitter-min/--sleep-jitter-max не могут быть отрицательными.")
    if args.sleep_jitter_min > args.sleep_jitter_max:
        raise ValueError("--sleep-jitter-min не должен быть больше --sleep-jitter-max.")
    if args.date_filter_mode not in {"auto", "server", "client"}:
        raise ValueError("--date-filter-mode должен быть auto, server или client.")
    if args.slice_by not in {"none", "year", "month"}:
        raise ValueError("--slice-by должен быть none, year или month.")

    if not args.countries.strip() or args.countries.upper() == "ASK":
        countries = ask_countries_interactive()
    else:
        countries = normalize_countries(args.countries)

    if args.updated_from is None:
        updated_from = ask_updated_from_interactive()
    else:
        updated_from = normalize_utc_timestamp(args.updated_from)
    updated_from_dt = parse_iso_datetime_utc(updated_from) if updated_from else None
    LOGGER.info(
        "Фильтр нормализован: countries=%s updated_from=%s",
        ",".join(countries),
        updated_from if updated_from else "без фильтра",
    )

    print(f"Страны для выгрузки: {', '.join(countries)}")
    print(f"Фильтр 'обновлено с': {updated_from if updated_from else 'без фильтра'}")

    slice_start_dt = parse_yyyy_mm_dd(args.slice_start, "--slice-start")
    slice_end_raw = args.slice_end if args.slice_end else datetime.now(timezone.utc).strftime("%Y-%m-%d")
    slice_end_dt = parse_yyyy_mm_dd(slice_end_raw, "--slice-end")
    if slice_end_dt < slice_start_dt:
        raise ValueError("--slice-end не может быть раньше --slice-start.")

    time_slices = iter_time_slices(args.slice_by, slice_start_dt, slice_end_dt)
    if not time_slices:
        raise ValueError("Не удалось построить временные интервалы выгрузки.")
    LOGGER.info("Сформированы интервалы: %s", ", ".join(label for label, _, _ in time_slices))

    if args.reset_state and os.path.exists(args.state_file):
        os.remove(args.state_file)
        LOGGER.info("State-файл сброшен: %s", args.state_file)

    state = load_state(args.state_file) if args.resume else {"countries": {}}
    signature = {
        "countries": countries,
        "updated_from": updated_from,
        "date_filter_mode": args.date_filter_mode,
        "slice_by": args.slice_by,
        "slice_date_field": args.slice_date_field,
        "slice_start": args.slice_start,
        "slice_end": slice_end_raw,
    }
    if args.resume and state.get("signature") and state["signature"] != signature:
        raise ValueError(
            "State-файл создан для других параметров выгрузки. "
            "Используйте --reset-state или укажите другой --state-file."
        )
    state["signature"] = signature

    def persist_country_state(
        state_key: str,
        next_skip: int,
        written: int,
        done: bool,
        client_filter_active: bool,
    ) -> None:
        countries_state = state.setdefault("countries", {})
        countries_state[state_key] = {
            "next_skip": next_skip,
            "written_in_run": written,
            "done": done,
            "client_filter_active": client_filter_active,
            "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        save_state(args.state_file, state)

    filename = output_name(countries, updated_from, args.output)
    if args.resume and args.output and os.path.exists(filename):
        raise ValueError(
            f"Файл {filename} уже существует. При --resume укажите другое --output, "
            "чтобы не перезаписать ранее выгруженные данные."
        )
    writer = CsvPartWriter(filename, args.max_rows_per_file, OUTPUT_COLUMNS)
    session = create_http_session(args.request_retries, args.user_agent)

    total = 0
    try:
        for country in countries:
            for slice_label, slice_start, slice_end in time_slices:
                state_key = f"{country}|{slice_label}"
                slice_clauses = build_slice_clauses(args.slice_date_field, slice_start, slice_end)
                country_state = state.get("countries", {}).get(state_key, {})
                if args.resume and country_state.get("done") is True:
                    LOGGER.info("%s: пропуск, интервал %s уже завершен по state-файлу.", country, slice_label)
                    continue
                start_skip = int(country_state.get("next_skip", 0)) if args.resume else 0
                if start_skip > 0:
                    LOGGER.info(
                        "%s: продолжаю интервал %s с skip=%s по state-файлу.",
                        country,
                        slice_label,
                        start_skip,
                    )

                total += stream_country(
                    session=session,
                    country_code=country,
                    limit=args.limit,
                    sleep_seconds=args.sleep,
                    updated_from=updated_from,
                    updated_from_dt=updated_from_dt,
                    date_filter_mode=args.date_filter_mode,
                    start_skip=start_skip,
                    extra_clauses=slice_clauses,
                    slice_label=slice_label,
                    request_timeout=args.request_timeout,
                    jitter_min=args.sleep_jitter_min,
                    jitter_max=args.sleep_jitter_max,
                    writer=writer,
                    progress_callback=lambda c, n, w, d, client, key=state_key: persist_country_state(
                        key,
                        n,
                        w,
                        d,
                        client,
                    ),
                )
    finally:
        writer.close()
        session.close()

    if total == 0:
        print("Данные не получены.")
        return

    if len(writer.files_created) == 1:
        print(f"\nИтог: {total} записей сохранено в {writer.files_created[0]}")
    else:
        print(f"\nИтог: {total} записей сохранено в {len(writer.files_created)} файлов.")
        for path in writer.files_created:
            print(f" - {path}")
    LOGGER.info("Завершено: записей=%s файлов=%s", total, len(writer.files_created))


if __name__ == "__main__":
    main()
