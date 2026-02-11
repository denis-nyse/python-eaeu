import argparse
import ast
import json
from pathlib import Path

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Преобразование выгрузки ЕАЭС CSV в более читаемый вид."
    )
    parser.add_argument("--input", required=True, help="Путь к исходному CSV (sep=';').")
    parser.add_argument(
        "--output",
        default="",
        help="Путь к выходному CSV. Если не указан, добавляется суффикс _readable.",
    )
    parser.add_argument(
        "--drop-empty-threshold",
        type=float,
        default=0.95,
        help=(
            "Порог доли пустых значений для удаления колонки "
            "(по умолчанию 0.95 = 95%% пустых)."
        ),
    )
    return parser.parse_args()


def parse_structured_value(value: str):
    text = value.strip()
    if not text:
        return ""
    if text in {"[]", "{}", "None", "nan", "NaN", "null"}:
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


def compact_scalar(value) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text in {"None", "nan", "NaN", "null", "[]", "{}"}:
        return ""
    return text


def flatten_for_humans(value) -> str:
    if isinstance(value, list):
        if not value:
            return ""
        parts = [flatten_for_humans(item) for item in value]
        parts = [p for p in parts if p]
        return " | ".join(parts)
    if isinstance(value, dict):
        pairs = []
        for key, raw in value.items():
            flat = flatten_for_humans(raw)
            if flat:
                pairs.append(f"{key}: {flat}")
        return "; ".join(pairs)
    return compact_scalar(value)


def looks_like_iso_date_column(column: str) -> bool:
    return column.endswith(".$date")


def human_column_name(column: str) -> str:
    name = column.replace(".$date", " date")
    name = name.replace(".", " / ")
    return name


def default_output_path(input_path: Path) -> Path:
    return input_path.with_name(f"{input_path.stem}_readable{input_path.suffix}")


def main() -> None:
    args = parse_args()

    input_path = Path(args.input).expanduser().resolve()
    output_path = (
        Path(args.output).expanduser().resolve()
        if args.output
        else default_output_path(input_path)
    )

    df = pd.read_csv(input_path, sep=";", dtype=str)

    for col in df.columns:
        df[col] = df[col].fillna("").map(parse_structured_value).map(flatten_for_humans)

        if looks_like_iso_date_column(col):
            parsed = pd.to_datetime(df[col], errors="coerce", utc=True)
            # Формат без секунды для компактности, если есть валидные даты.
            if parsed.notna().any():
                df[col] = parsed.dt.strftime("%Y-%m-%d %H:%M").fillna("")

    # Убираем технические колонки почти всегда нерелевантные для чтения.
    technical_prefixes = ("_sys", "_class", "_source", "masterId.$binary", "_id.$oid")
    keep_cols = [
        c
        for c in df.columns
        if not any(c.startswith(prefix) for prefix in technical_prefixes)
    ]
    df = df[keep_cols]

    # Удаляем колонки, где слишком много пустых значений.
    empty_share = (df == "").sum() / len(df) if len(df) else 0
    drop_cols = [c for c in df.columns if float(empty_share[c]) >= args.drop_empty_threshold]
    if drop_cols:
        df = df.drop(columns=drop_cols)

    # Переименовываем колонки в более читаемый формат.
    df = df.rename(columns={c: human_column_name(c) for c in df.columns})

    # Полезные колонки ставим в начало, если они есть.
    preferred = [
        "unifiedCountryCode / value",
        "docId",
        "formNumberId",
        "conformityDocKindName",
        "docStartDate date",
        "docValidityDate date",
        "applicantDetails / businessEntityName",
    ]
    first = [c for c in preferred if c in df.columns]
    other = [c for c in df.columns if c not in first]
    df = df[first + other]

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, sep=";", index=False, encoding="utf-8-sig")
    print(f"Готово: {output_path}")
    print(f"Строк: {len(df)} | Колонок: {len(df.columns)}")


if __name__ == "__main__":
    main()
