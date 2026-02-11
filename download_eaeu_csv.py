import argparse
import ast
import json
import time
from datetime import datetime, timezone

import pandas as pd
import requests

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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Выгрузка данных ЕАЭС через REST API в CSV. "
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
    parser.add_argument(
        "--readable-output",
        action="store_true",
        help="Дополнительно сохранять расширенный читаемый CSV со всеми доступными полями.",
    )
    parser.add_argument(
        "--readable-drop-empty-threshold",
        type=float,
        default=0.95,
        help="Удалять колонки, где доля пустых значений >= порога (по умолчанию 0.95).",
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

    # Удаляем дубликаты с сохранением порядка
    return list(dict.fromkeys(countries))


def fetch_data_for_country(country_code: str, limit: int, sleep_seconds: float) -> list[object]:
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

    all_data: list[object] = []
    skip = 0
    batch_count = 1

    print(f"\nСтарт выгрузки страны {country_code} (limit={limit})")

    while True:
        url = f"{BASE_URL}?collection={COLLECTION_NAME}&limit={limit}&skip={skip}"
        response = requests.post(
            url,
            headers=headers,
            data=json.dumps(query_payload),
            timeout=60,
        )
        response.raise_for_status()

        payload = response.json()

        if isinstance(payload, list):
            data = payload
        elif isinstance(payload, dict):
            # На разных стендах API может возвращать данные в обертке.
            data = (
                payload.get("data")
                or payload.get("items")
                or payload.get("result")
                or []
            )
            if not isinstance(data, list):
                data = [data]
        else:
            data = [payload]

        if not data:
            print(f"{country_code}: данные закончились.")
            break

        print(
            f"{country_code} | пачка #{batch_count}: {len(data)} записей "
            f"(skip={skip})"
        )
        all_data.extend(data)

        if len(data) < limit:
            print(f"{country_code}: последняя пачка.")
            break

        skip += limit
        batch_count += 1
        time.sleep(sleep_seconds)

    print(f"{country_code}: всего получено {len(all_data)} записей.")
    return all_data


def records_to_dicts(records: list[object], country_code: str) -> list[dict]:
    normalized: list[dict] = []

    for index, item in enumerate(records):
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
                row = {
                    "_raw_value": item,
                    "_raw_type": "str",
                }
        elif isinstance(item, list):
            row = {
                "_raw_value": json.dumps(item, ensure_ascii=False),
                "_raw_type": "list",
            }
        else:
            row = {
                "_raw_value": item,
                "_raw_type": type(item).__name__,
            }

        # Гарантируем поле страны для дальнейшей сортировки и анализа.
        row.setdefault(COUNTRY_FIELD, country_code)
        row.setdefault("_source_country", country_code)
        row.setdefault("_source_index", index)
        normalized.append(row)

    return normalized


def ensure_dict_records(records: list[object]) -> list[dict]:
    safe: list[dict] = []
    for index, item in enumerate(records):
        if isinstance(item, dict):
            safe.append(item)
        else:
            safe.append(
                {
                    "_raw_value": item,
                    "_raw_type": type(item).__name__,
                    "_source_index": index,
                }
            )
    return safe


def output_name(countries: list[str], explicit_name: str) -> str:
    if explicit_name:
        return explicit_name
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix = "ALL" if set(countries) == set(VALID_COUNTRY_CODES) else "_".join(countries)
    return f"export_{suffix}_{stamp}.csv"


def readable_output_name(filename: str) -> str:
    if "." in filename:
        base, ext = filename.rsplit(".", 1)
        return f"{base}_readable.{ext}"
    return f"{filename}_readable.csv"


def parse_structured_value(value: str):
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


def to_ddmmyyyy(value: str) -> str:
    text = value.strip() if isinstance(value, str) else ""
    if not text:
        return ""
    dt = pd.to_datetime(text, errors="coerce", utc=True)
    if pd.isna(dt):
        return ""
    return dt.strftime("%d.%m.%Y")


def extract_from_structured(value: str, key: str) -> str:
    parsed = parse_structured_value(value if isinstance(value, str) else "")
    if isinstance(parsed, dict):
        val = parsed.get(key, "")
        return flatten_for_humans(val)
    if isinstance(parsed, list):
        values = []
        for item in parsed:
            if isinstance(item, dict) and key in item:
                text = flatten_for_humans(item.get(key))
                if text:
                    values.append(text)
        if values:
            # Убираем дубликаты с сохранением порядка.
            return " | ".join(list(dict.fromkeys(values)))
    return ""


def status_from_row(row: pd.Series) -> str:
    end_date_raw = str(row.get("docValidityDate.$date", "") or "").strip()
    status_code = str(row.get("docStatusDetails.docStatusCode", "") or "").strip()
    note_text = str(row.get("docStatusDetails.noteText", "") or "").strip().lower()

    end_dt = pd.to_datetime(end_date_raw, errors="coerce", utc=True)
    now = datetime.now(timezone.utc)
    if pd.notna(end_dt):
        return "действует" if end_dt >= now else "прекращен"

    if "прекращ" in note_text:
        return "прекращен"

    if status_code in {"09", "10"}:
        return "прекращен"
    if status_code:
        return "действует"
    return ""


def to_selected_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()

    work["Регистрационный номер документа"] = work.get("docId", "").fillna("").astype(str)
    work["Страна"] = (
        work.get(COUNTRY_FIELD, "").fillna("").astype(str).map(lambda x: COUNTRY_NAMES_RU.get(x.strip(), x.strip()))
    )
    work["Вид документа"] = work.get("conformityDocKindName", "").fillna("").astype(str)

    start = work.get("docStartDate.$date", "").fillna("").astype(str).map(to_ddmmyyyy)
    end = work.get("docValidityDate.$date", "").fillna("").astype(str).map(to_ddmmyyyy)
    work["Срок действия"] = [
        f"{s} - {e}" if s and e else (s or e) for s, e in zip(start.tolist(), end.tolist())
    ]

    work["Заявитель"] = work.get("applicantDetails.businessEntityName", "").fillna("").astype(str)

    manufacturers = work.get("technicalRegulationObjectDetails.manufacturerDetails", "").fillna("").astype(str)
    work["Изготовитель"] = manufacturers.map(lambda v: extract_from_structured(v, "businessEntityName"))
    work["Изготовитель"] = work["Изготовитель"].where(
        work["Изготовитель"].str.strip().astype(bool),
        work.get("applicantDetails.businessEntityName", "").fillna("").astype(str),
    )

    work["Технический регламент"] = (
        work.get("technicalRegulationId", "").fillna("").astype(str).map(parse_structured_value).map(flatten_for_humans)
    )

    work["Наименование органа по оценке соответствия"] = (
        work.get("conformityAuthorityV2Details.businessEntityName", "").fillna("").astype(str)
    )

    work["Статус действия"] = work.apply(status_from_row, axis=1)

    selected_cols = [
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
    result = work[selected_cols].copy()
    result = result.fillna("")
    return result


def to_readable_dataframe(df: pd.DataFrame, empty_threshold: float) -> pd.DataFrame:
    readable = df.copy()

    for col in readable.columns:
        readable[col] = readable[col].fillna("").map(parse_structured_value).map(flatten_for_humans)

        if col.endswith(".$date"):
            parsed = pd.to_datetime(readable[col], errors="coerce", utc=True)
            if parsed.notna().any():
                readable[col] = parsed.dt.strftime("%Y-%m-%d %H:%M").fillna("")

    technical_prefixes = ("_sys", "_class", "_source", "masterId.$binary", "_id.$oid")
    keep_cols = [
        c for c in readable.columns if not any(c.startswith(prefix) for prefix in technical_prefixes)
    ]
    readable = readable[keep_cols]

    if len(readable):
        empty_share = (readable == "").sum() / len(readable)
        drop_cols = [c for c in readable.columns if float(empty_share[c]) >= empty_threshold]
        if drop_cols:
            readable = readable.drop(columns=drop_cols)

    rename_map = {c: c.replace(".$date", " date").replace(".", " / ") for c in readable.columns}
    readable = readable.rename(columns=rename_map)

    preferred = [
        "unifiedCountryCode / value",
        "docId",
        "formNumberId",
        "conformityDocKindName",
        "docStartDate date",
        "docValidityDate date",
        "applicantDetails / businessEntityName",
    ]
    first = [c for c in preferred if c in readable.columns]
    other = [c for c in readable.columns if c not in first]
    return readable[first + other]


def save_csv_in_parts(df: pd.DataFrame, filename: str, max_rows_per_file: int) -> None:
    if max_rows_per_file <= 0:
        raise ValueError("--max-rows-per-file должен быть больше 0.")

    if len(df) <= max_rows_per_file:
        df.to_csv(filename, index=False, sep=";", encoding="utf-8-sig")
        print(f"\nИтог: {len(df)} записей сохранено в {filename}")
        return

    if "." in filename:
        base, ext = filename.rsplit(".", 1)
        ext = f".{ext}"
    else:
        base, ext = filename, ".csv"

    total_parts = (len(df) + max_rows_per_file - 1) // max_rows_per_file
    for part_index in range(total_parts):
        start = part_index * max_rows_per_file
        end = min(start + max_rows_per_file, len(df))
        chunk = df.iloc[start:end]
        part_name = f"{base}_part{part_index + 1:03d}{ext}"
        chunk.to_csv(part_name, index=False, sep=";", encoding="utf-8-sig")
        print(
            f"Сохранен файл {part_name}: строк {len(chunk)} "
            f"(диапазон {start + 1}-{end})"
        )

    print(f"\nИтог: {len(df)} записей сохранено в {total_parts} файлов.")


def main() -> None:
    args = parse_args()

    if not args.countries.strip() or args.countries.upper() == "ASK":
        countries = ask_countries_interactive()
    else:
        countries = normalize_countries(args.countries)

    print(f"Страны для выгрузки: {', '.join(countries)}")

    all_records: list[dict] = []
    for country in countries:
        country_records = fetch_data_for_country(country, args.limit, args.sleep)
        all_records.extend(records_to_dicts(country_records, country))

    if not all_records:
        print("Данные не получены.")
        return

    all_records = ensure_dict_records(all_records)
    df = pd.json_normalize(all_records)
    if COUNTRY_FIELD in df.columns:
        df = df.sort_values(by=[COUNTRY_FIELD], ascending=True, kind="stable")

    filename = output_name(countries, args.output)
    selected_df = to_selected_dataframe(df)
    save_csv_in_parts(selected_df, filename, args.max_rows_per_file)

    if args.readable_output:
        readable_df = to_readable_dataframe(df, args.readable_drop_empty_threshold)
        readable_name = readable_output_name(filename)
        save_csv_in_parts(readable_df, readable_name, args.max_rows_per_file)


if __name__ == "__main__":
    main()
