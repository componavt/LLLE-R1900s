import os
from pathlib import Path
from typing import Tuple, Optional
import re
from dotenv import load_dotenv

# Загружаем переменные окружения из .env в корне проекта
load_dotenv()

# Получаем пути из .env
CSV_IN_DIR = os.getenv("CSV_IN_DIR", "data/csv_in")
PARQUET_PATH = os.getenv("PARQUET_PATH", "data/data.parquet")
OUTPUTS_DIR = os.getenv("OUTPUTS_DIR", "outputs")


def parse_filename(filename: str) -> Optional[Tuple[str, str, str]]:
    """
    Парсит имя файла вида:
        "ГОД ПОСЕЛЕНИЕ Месяц1-Месяц2.csv"
        "ГОД ПОСЕЛЕНИЕ Месяц1.csv"

    Примеры:
        '1916 Ивинского 01-03.csv' → ('1916', 'Ивинского', '01-03')
        '1916 Кузарандского 02.csv' → ('1916', 'Кузарандского', '02')

    Возвращает кортеж (год, поселение, период) или None, если не удалось распарсить.
    """
    # Убираем расширение .csv
    if not filename.endswith(".csv"):
        return None

    stem = filename[:-4]  # удаляем последние 4 символа: ".csv"

    # Регулярное выражение: год (4 цифры), затем пробел, затем всё до последнего пробела — поселение,
    # затем последний фрагмент — период (месяцы)
    pattern = r"^(\d{4})\s+(.+)\s+(\d{1,2}(?:-\d{1,2})?)$"
    match = re.match(pattern, stem)

    if not match:
        print(f"⚠️  Не удалось распарсить имя файла: {filename}")
        return None

    year, settlement, period = match.groups()

    # Валидация года
    if not (1900 <= int(year) <= 1999):
        print(f"❌ Некорректный год в файле: {filename}")
        return None

    # Валидация периода
    if "-" in period:
        start, end = period.split("-")
        if not (1 <= int(start) <= 12) or not (1 <= int(end) <= 12):
            print(f"❌ Некорректный период в файле: {filename}")
            return None
    else:
        if not (1 <= int(period) <= 12):
            print(f"❌ Некорректный месяц в файле: {filename}")
            return None

    return year, settlement, period


def main():
    """
    Основная функция: обходит все CSV-файлы в CSV_IN_DIR и парсит их имена.
    """
    csv_dir = Path(CSV_IN_DIR)

    if not csv_dir.exists():
        print(f"❌ Папка {CSV_IN_DIR} не существует!")
        return

    print(f"📂 Сканирую папку: {csv_dir.absolute()}")

    for csv_file in csv_dir.rglob("*.csv"):
        rel_path = csv_file.relative_to(csv_dir)
        result = parse_filename(csv_file.name)

        if result:
            year, settlement, period = result
            print(f"✅ {rel_path} → Год: {year}, Поселение: {settlement}, Период: {period}")
        else:
            print(f"❌ Пропуск: {rel_path}")


if __name__ == "__main__":
    main()
