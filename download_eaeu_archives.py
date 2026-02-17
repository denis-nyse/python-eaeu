import gzip
import json
import csv
import logging
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone

# Константы
INDEX_URL = "https://tech.eaeunion.org/rest-api-data/35-1/"
TARGET_COUNTRY = "KG"
OUTPUT_FILE = f"eaeu_archive_export_{TARGET_COUNTRY}.csv"

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("archive_processor")

OUTPUT_COLUMNS = [
    "Регистрационный номер документа", "Страна", "Вид документа", "Срок действия",
    "Заявитель", "Изготовитель", "Технический регламент", 
    "Наименование органа по оценке соответствия", "Статус действия",
]

COUNTRY_NAMES_RU = {"AM": "Армения", "BY": "Беларусь", "KG": "Кыргызстан", "KZ": "Казахстан", "RU": "Россия"}

def get_nested(obj, path):
    current = obj
    for part in path.split("."):
        if isinstance(current, dict) and part in current:
            current = current[part]
        else: return ""
    return current

def flatten_for_humans(value):
    if isinstance(value, list):
        items = [flatten_for_humans(item) for item in value if item]
        return " | ".join(items)
    if isinstance(value, dict):
        parts = [f"{k}: {flatten_for_humans(v)}" for k, v in value.items() if v]
        return "; ".join(parts)
    text = str(value).strip()
    return "" if text in {"None", "nan", "null", "[]", "{}"} else text

def status_from_record(record):
    # Упрощенная логика статуса для архивов
    status_code = str(get_nested(record, "docStatusDetails.docStatusCode"))
    note = str(get_nested(record, "docStatusDetails.noteText")).lower()
    if "прекращ" in note or status_code in {"09", "10"}: return "прекращен"
    return "действует" if status_code else "неизвестно"

def record_to_row(record):
    applicant = flatten_for_humans(get_nested(record, "applicantDetails.businessEntityName"))
    manuf_list = get_nested(record, "technicalRegulationObjectDetails.manufacturerDetails")
    manufacturer = ""
    if isinstance(manuf_list, list) and manuf_list:
        manufacturer = flatten_for_humans(manuf_list[0].get("businessEntityName"))
    
    # Даты в архивах могут быть в разных вложенностях, пробуем достать
    start = flatten_for_humans(record.get("docStartDate", ""))[:10]
    end = flatten_for_humans(record.get("docValidityDate", ""))[:10]
    
    return {
        "Регистрационный номер документа": flatten_for_humans(record.get("docId")),
        "Страна": COUNTRY_NAMES_RU.get(TARGET_COUNTRY, TARGET_COUNTRY),
        "Вид документа": flatten_for_humans(record.get("conformityDocKindName")),
        "Срок действия": f"{start} - {end}" if start and end else (start or end),
        "Заявитель": applicant,
        "Изготовитель": manufacturer or applicant,
        "Технический регламент": flatten_for_humans(record.get("technicalRegulationId")),
        "Наименование органа по оценке соответствия": flatten_for_humans(
            get_nested(record, "conformityAuthorityV2Details.businessEntityName")
        ),
        "Статус действия": status_from_record(record),
    }

def process_archives():
    logger.info(f"Сканируем список архивов...")
    try:
        r = requests.get(INDEX_URL, timeout=30)
        r.raise_for_status()
    except Exception as e:
        logger.error(f"Не удалось получить список файлов: {e}")
        return

    soup = BeautifulSoup(r.text, "html.parser")
    links = [INDEX_URL + a["href"] for a in soup.find_all("a") if a["href"].endswith(".json.gz")]
    logger.info(f"Найдено файлов: {len(links)}")

    total_kg = 0
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS, delimiter=";")
        writer.writeheader()

        for i, url in enumerate(links):
            fname = url.split("/")[-1]
            logger.info(f"[{i+1}/{len(links)}] Качаем и фильтруем: {fname}")
            
            try:
                # stream=True позволяет не грузить файл в память целиком до распаковки
                with requests.get(url, stream=True, timeout=60) as resp:
                    resp.raise_for_status()
                    # Распаковываем "на лету" из потока
                    with gzip.GzipFile(fileobj=resp.raw) as gz:
                        data = json.load(gz)
                
                records = data.get("result", []) if isinstance(data, dict) else data
                count = 0
                for rec in records:
                    if get_nested(rec, "unifiedCountryCode.value") == TARGET_COUNTRY:
                        writer.writerow(record_to_row(rec))
                        count += 1
                
                total_kg += count
                if count > 0:
                    logger.info(f"   Найдено в файле: {count} (Всего KG: {total_kg})")
            
            except Exception as e:
                logger.error(f"   Ошибка в файле {fname}: {e}")

    logger.info(f"ГОТОВО! Файл сохранен: {OUTPUT_FILE}. Найдено записей: {total_kg}")

if __name__ == "__main__":
    process_archives()