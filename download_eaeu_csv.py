import argparse
import ast
import csv
import json
import time
from datetime import datetime, timezone

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://tech.eaeunion.org/spd/find"
COLLECTION_NAME = "kbdallread.service-prop-35_1-conformityDocDetailsType"
VALID_COUNTRY_CODES = ["AM", "BY", "KG", "KZ", "RU"]
COUNTRY_FIELD = "unifiedCountryCode.value"
DEFAULT_LIMIT = 10000
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
REQUEST_TIMEOUT_SECONDS = 60
MAX_REQUEST_RETRIES = 6
RETRY_BACKOFF_SECONDS = 1.0


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
    parser = argparse.ArgumentParser(
        description=(
            "Потоковая выгрузка данных ЕАЭС через REST API в CSV. "
            "Поддерживает выбор одной, нескольких или всех стран."
        )
    )
    parser.add_argument(
        "--countries",
        type=str,
        default="",
        help="Коды стран через запятую (например: RU,BY), ALL или ASK. Если не указано, будет интерактивный выбор.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help=f"Размер пачки (по умолчанию {DEFAULT_LIMIT}).",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=1.0,
        help="Пауза между запросами в секундах (по умолчанию 1.0).",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="",
        help="Имя выходного CSV файла. Если не указано, имя будет сгенерировано автоматически.",
    )
    parser.add_argument(
        "--max-rows-per-file",
        type=int,
        default=10000,
        help="Максимум строк в одном CSV файле (по умолчанию 10000).",
    )
    return parser.parse_args()


def create_http_session() -> requests.Session:
    retry = Retry(
        total=MAX_REQUEST_RETRIES,
        connect=MAX_REQUEST_RETRIES,
        read=MAX_REQUEST_RETRIES,
        backoff_factor=RETRY_BACKOFF_SECONDS,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset({"POST"}),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


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


def output_name(countries: list[str], explicit_name: str) -> str:
    if explicit_name:
        return explicit_name
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix = "ALL" if set(countries) == set(VALID_COUNTRY_CODES) else "_".join(countries)
    return f"export_{suffix}_{stamp}.csv"


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


def get_value(record: dict, path: str, default=""):
    if path in record:
        return record.get(path, default)
    return get_nested(record, path, default)


def get_date_value(record: dict, field_name: str):
    direct = get_value(record, field_name, "")
    if isinstance(direct, dict):
        nested = direct.get("$date", "")
        if nested:
            return nested
    if direct:
        return direct

    dotted = get_value(record, f"{field_name}.$date", "")
    if dotted:
        return dotted

    return ""


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
    status_code = flatten_for_humans(get_value(record, "docStatusDetails.docStatusCode", "")).strip()
    note_text = flatten_for_humans(get_value(record, "docStatusDetails.noteText", "")).strip().lower()

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
    country_code = flatten_for_humans(get_value(record, COUNTRY_FIELD, "")).strip()

    applicant = flatten_for_humans(get_value(record, "applicantDetails.businessEntityName", ""))
    manufacturer = extract_from_structured(
        get_value(record, "technicalRegulationObjectDetails.manufacturerDetails", ""),
        "businessEntityName",
    )
    if not manufacturer:
        manufacturer = applicant

    start = to_ddmmyyyy(get_date_value(record, "docStartDate"))
    end = to_ddmmyyyy(get_date_value(record, "docValidityDate"))
    term = f"{start} - {end}" if start and end else (start or end)

    return {
        "Регистрационный номер документа": flatten_for_humans(get_value(record, "docId", "")),
        "Страна": COUNTRY_NAMES_RU.get(country_code, country_code),
        "Вид документа": flatten_for_humans(get_value(record, "conformityDocKindName", "")),
        "Срок действия": term,
        "Заявитель": applicant,
        "Изготовитель": manufacturer,
        "Технический регламент": flatten_for_humans(
            parse_structured_value(get_value(record, "technicalRegulationId", ""))
        ),
        "Наименование органа по оценке соответствия": flatten_for_humans(
            get_value(record, "conformityAuthorityV2Details.businessEntityName", "")
        ),
        "Статус действия": status_from_record(record),
    }


def extract_rest_data(payload: object) -> list[object]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        data = payload.get("data") or payload.get("items") or payload.get("result") or []
        return data if isinstance(data, list) else [data]
    return [payload]


def fetch_batch(
    session: requests.Session,
    country_code: str,
    limit: int,
    skip: int,
) -> list[object]:
    headers = {"Content-Type": "text/plain"}
    query_payload = {
        "$and": [
            {
                COUNTRY_FIELD: {
                    "$eq": country_code,
                }
            }
        ]
    }
    url = f"{BASE_URL}?collection={COLLECTION_NAME}&limit={limit}&skip={skip}"
    response = session.post(
        url,
        headers=headers,
        data=json.dumps(query_payload),
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    return extract_rest_data(response.json())


def stream_country(
    session: requests.Session,
    country_code: str,
    limit: int,
    sleep_seconds: float,
    writer: CsvPartWriter,
) -> int:
    total_written = 0
    skip = 0
    batch_count = 1
    server_page_cap_detected = False

    print(f"\nСтарт выгрузки страны {country_code} (limit={limit})")

    while True:
        try:
            data = fetch_batch(session, country_code, limit, skip)
        except requests.RequestException as exc:
            print(
                f"{country_code}: ошибка сети на skip={skip}: {exc}. "
                "Повторю через паузу."
            )
            time.sleep(max(2.0, sleep_seconds))
            continue

        if not data:
            print(f"{country_code}: данные закончились.")
            break

        rows = []
        for item in data:
            record = normalize_record(item, country_code)
            rows.append(record_to_selected_row(record))

        if rows:
            writer.write_rows(rows)
            total_written += len(rows)

        print(
            f"{country_code} | пачка #{batch_count}: получено {len(data)} записей "
            f"(skip={skip}), записано {len(rows)}"
        )

        if len(data) < limit and not server_page_cap_detected:
            print(
                f"{country_code}: сервер вернул {len(data)} < limit={limit}. "
                "Продолжаю пагинацию до пустого ответа."
            )
            server_page_cap_detected = True

        skip += len(data)
        batch_count += 1
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

    print(f"{country_code}: всего записано {total_written} строк.")
    return total_written


def main() -> None:
    args = parse_args()

    if args.limit <= 0:
        raise ValueError("--limit должен быть больше 0.")

    if not args.countries.strip() or args.countries.upper() == "ASK":
        countries = ask_countries_interactive()
    else:
        countries = normalize_countries(args.countries)

    print(f"Страны для выгрузки: {', '.join(countries)}")

    filename = output_name(countries, args.output)
    writer = CsvPartWriter(filename, args.max_rows_per_file, OUTPUT_COLUMNS)
    session = create_http_session()

    total = 0
    try:
        for country in countries:
            total += stream_country(
                session=session,
                country_code=country,
                limit=args.limit,
                sleep_seconds=args.sleep,
                writer=writer,
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


if __name__ == "__main__":
    main()
