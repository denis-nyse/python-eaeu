import argparse
import glob
import os
import re


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Склейка CSV файлов в один (заголовок берется только из первого файла)."
    )
    parser.add_argument(
        "--pattern",
        required=True,
        help="Шаблон входных файлов, например 'export_KG_*.csv'.",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Имя итогового CSV файла.",
    )
    return parser.parse_args()


def part_sort_key(path: str) -> tuple[str, int]:
    name = os.path.basename(path)
    match = re.search(r"_part(\d+)\.csv$", name, flags=re.IGNORECASE)
    if match:
        return (re.sub(r"_part\d+\.csv$", "", name, flags=re.IGNORECASE), int(match.group(1)))
    if name.lower().endswith(".csv"):
        return (name[:-4], 1)
    return (name, 1)


def merge_csv(files: list[str], output_path: str) -> None:
    if not files:
        raise ValueError("Не найдено файлов для склейки.")

    sorted_files = sorted(files, key=part_sort_key)
    print("Файлы к склейке:")
    for path in sorted_files:
        print(f" - {path}")

    total_rows = 0
    with open(output_path, "wb") as out:
        for i, path in enumerate(sorted_files):
            with open(path, "rb") as src:
                if i == 0:
                    for line_no, line in enumerate(src):
                        out.write(line)
                        if line_no > 0:
                            total_rows += 1
                    continue

                # В остальных файлах пропускаем первую строку (заголовок).
                _ = src.readline()
                for line in src:
                    out.write(line)
                    total_rows += 1

    print(f"\nГотово: {output_path}")
    print(f"Строк данных (без заголовка): {total_rows}")


def main() -> None:
    args = parse_args()
    files = glob.glob(args.pattern)
    merge_csv(files, args.output)


if __name__ == "__main__":
    main()
