import argparse
import ast
import csv
import json
import time
from datetime import datetime, timezone

import requests

BASE_URL = "https://opendata.eaeunion.org/odata/conformityDocDetailsType"
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
        "--start",
        type=int,
        default=None,
        help="С какой записи начинать (включительно).",
    )
    parser.add_argument(
        "--end",
        type=int,
        default=None,
        help="По какую запись выгружать (включительно).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=2000,
        help="Размер пачки одного запроса (по умолчанию 2000, максимум 10000).",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.2,
        help="Пауза между запросами в секундах (по умолчанию 0.2).",
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
        "--year-from",
        type=int,
        default=None,
        help="Фильтр: действует с указанного года (например 2020).",
    )
    return parser.parse_args()


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


def ask_range_interactive() -> tuple[int, int]:
    print("Выберите диапазон записей для выгрузки:")
    start_raw = input("С какой записи начать (включительно, например 0): ").strip()
    end_raw = input("По какую запись выгружать (включительно, например 20000): ").strip()

    if not start_raw or not end_raw:
        raise ValueError("Нужно указать и начало, и конец диапазона.")

    start = int(start_raw)
    end = int(end_raw)
    if start < 0 or end < 0:
        raise ValueError("Диапазон не может быть отрицательным.")
    if end < start:
        raise ValueError("Конец диапазона должен быть больше или равен началу.")
    return start, end


def ask_year_from_interactive() -> int | None:
    print("Фильтр по дате начала действия:")
    print("1. Без фильтра по году")
    print("2. Действует с выбранного года")
    choice = input("Введите номер (1-2): ").strip()
    if choice == "1":
        return None
    if choice != "2":
        raise ValueError("Неверный выбор. Используйте 1 или 2.")

    year_raw = input("Введите год (например 2020): ").strip()
    if not year_raw:
        raise ValueError("Год не указан.")
    year = int(year_raw)
    if year < 1900 or year > 2100:
        raise ValueError("Год вне допустимого диапазона (1900-2100).")
    return year


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


def output_name(countries: list[str], start: int, end: int, explicit_name: str) -> str:
    if explicit_name:
        return explicit_name
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix = "ALL" if set(countries) == set(VALID_COUNTRY_CODES) else "_".join(countries)
    return f"export_odata_{suffix}_{start}_{end}_{stamp}.csv"


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
    # ODATA может вернуть дату как:
    # 1) field_name: "2024-01-01T00:00:00Z"
    # 2) field_name: {"$date": "..."}
    # 3) field_name.$date: "..."
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


def build_odata_filter(country_code: str, year_from: int | None) -> str:
    clauses = [f"unifiedCountryCode/value eq '{country_code}'"]
    if year_from is not None:
        clauses.append(f"docStartDate ge {year_from}-01-01T00:00:00Z")
    return " and ".join(clauses)


def fetch_batch(country_code: str, skip: int, top: int, year_from: int | None) -> list[object]:
    params = {
        "$top": top,
        "$skip": skip,
        "$filter": build_odata_filter(country_code, year_from),
    }
    response = requests.get(BASE_URL, params=params, timeout=60)
    response.raise_for_status()
    return odata_extract_records(response.json())


def stream_country_range(
    country_code: str,
    start: int,
    end: int,
    limit: int,
    sleep_seconds: float,
    year_from: int | None,
    writer: CsvPartWriter,
) -> int:
    total_written = 0
    skip = start
    batch_count = 1

    print(
        f"\nСтарт выгрузки страны {country_code} в диапазоне {start}-{end} "
        f"(limit={limit}, действует с: {year_from if year_from else 'без фильтра'})"
    )

    while skip <= end:
        remaining = end - skip + 1
        top = min(limit, remaining)

        data = fetch_batch(country_code, skip, top, year_from)
        if not data:
            print(f"{country_code}: данные закончились на skip={skip}.")
            break

        rows = []
        for item in data:
            record = normalize_record(item, country_code)
            rows.append(record_to_selected_row(record))

        writer.write_rows(rows)
        total_written += len(rows)

        print(
            f"{country_code} | пачка #{batch_count}: получено {len(data)} записей "
            f"(skip={skip}, top={top})"
        )

        if len(data) < top:
            print(f"{country_code}: достигнут конец доступных данных.")
            break

        skip += len(data)
        batch_count += 1
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

    print(f"{country_code}: записано {total_written} строк.")
    return total_written


def main() -> None:
    args = parse_args()

    if args.limit <= 0:
        raise ValueError("--limit должен быть больше 0.")
    if args.limit > 10000:
        raise ValueError("--limit не должен быть больше 10000.")

    if not args.countries.strip() or args.countries.upper() == "ASK":
        countries = ask_countries_interactive()
    else:
        countries = normalize_countries(args.countries)

    if args.start is None or args.end is None:
        start, end = ask_range_interactive()
    else:
        start, end = args.start, args.end

    if args.year_from is None:
        year_from = ask_year_from_interactive()
    else:
        year_from = args.year_from

    if start < 0 or end < 0:
        raise ValueError("Диапазон не может быть отрицательным.")
    if end < start:
        raise ValueError("Конец диапазона должен быть больше или равен началу.")
    if year_from is not None and (year_from < 1900 or year_from > 2100):
        raise ValueError("--year-from вне допустимого диапазона (1900-2100).")

    print(f"Страны для выгрузки: {', '.join(countries)}")
    print(f"Диапазон записей: {start}-{end} (включительно)")
    print(f"Фильтр 'действует с': {year_from if year_from else 'без фильтра'}")

    filename = output_name(countries, start, end, args.output)
    writer = CsvPartWriter(filename, args.max_rows_per_file, OUTPUT_COLUMNS)

    total = 0
    try:
        for country in countries:
            total += stream_country_range(
                country_code=country,
                start=start,
                end=end,
                limit=args.limit,
                sleep_seconds=args.sleep,
                year_from=year_from,
                writer=writer,
            )
    finally:
        writer.close()

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
