import argparse
import json
import time
from datetime import datetime

import pandas as pd
import requests

BASE_URL = "https://tech.eaeunion.org/spd/find"
COLLECTION_NAME = "kbdallread.service-prop-35_1-conformityDocDetailsType"
VALID_COUNTRY_CODES = ["AM", "BY", "KG", "KZ", "RU"]
COUNTRY_FIELD = "unifiedCountryCode.value"
DEFAULT_LIMIT = 10000


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
        default="ALL",
        help="Коды стран через запятую (например: RU,BY) или ALL.",
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


def fetch_data_for_country(country_code: str, limit: int, sleep_seconds: float) -> list[dict]:
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

    all_data: list[dict] = []
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

        data = response.json()
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


def output_name(countries: list[str], explicit_name: str) -> str:
    if explicit_name:
        return explicit_name
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix = "ALL" if set(countries) == set(VALID_COUNTRY_CODES) else "_".join(countries)
    return f"export_{suffix}_{stamp}.csv"


def main() -> None:
    args = parse_args()

    if args.countries.upper() == "ASK":
        countries = ask_countries_interactive()
    else:
        countries = normalize_countries(args.countries)

    print(f"Страны для выгрузки: {', '.join(countries)}")

    all_records: list[dict] = []
    for country in countries:
        all_records.extend(fetch_data_for_country(country, args.limit, args.sleep))

    if not all_records:
        print("Данные не получены.")
        return

    df = pd.json_normalize(all_records)
    if COUNTRY_FIELD in df.columns:
        df = df.sort_values(by=[COUNTRY_FIELD], ascending=True, kind="stable")

    filename = output_name(countries, args.output)
    df.to_csv(filename, index=False, sep=";", encoding="utf-8-sig")

    print(f"\nИтог: {len(df)} записей сохранено в {filename}")


if __name__ == "__main__":
    main()
