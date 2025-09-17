import os
from pathlib import Path
from typing import Tuple, Optional
import re
from dotenv import load_dotenv

# Load environment variables from .env in project root
load_dotenv()

# Get paths from .env
CSV_IN_DIR = os.getenv("CSV_IN_DIR", "data/csv_in")
PARQUET_PATH = os.getenv("PARQUET_PATH", "data/data.parquet")
OUTPUTS_DIR = os.getenv("OUTPUTS_DIR", "outputs")


def parse_filename(filename: str) -> Optional[Tuple[str, str, str]]:
    """
    Parse filename in one of the following formats:
        "YEAR SETTLEMENT Month1-Month2.csv"
        "YEAR SETTLEMENT Month1.csv"
        "YEAR SETTLEMENT.csv"  → interpreted as full year: "01-12"

    Examples:
        '1916 Ивинского 01-03.csv' → ('1916', 'Ивинского', '01-03')
        '1916 Кузарандского 02.csv' → ('1916', 'Кузарандского', '02')
        '1917 Ладвинское.csv' → ('1917', 'Ладвинское', '01-12')

    Returns tuple (year, settlement, period) or None if parsing fails.
    """
    if not filename.endswith(".csv"):
        return None

    stem = filename[:-4]  # Remove ".csv" extension

    # Regex: year (4 digits), then space, then settlement (any characters),
    # optionally followed by space and period (months)
    pattern = r"^(\d{4})\s+(.+?)(?:\s+(\d{1,2}(?:-\d{1,2})?))?$"
    match = re.match(pattern, stem)

    if not match:
        print(f"⚠️  Failed to parse filename: {filename}")
        return None

    year, settlement, period = match.groups()

    # Validate year
    try:
        year_int = int(year)
        if not (1900 <= year_int <= 1999):
            print(f"❌ Invalid year in filename: {filename}")
            return None
    except ValueError:
        print(f"❌ Non-numeric year in filename: {filename}")
        return None

    # If period is not specified, assume full year
    if period is None:
        period = "01-12"

    # Validate period
    if "-" in period:
        parts = period.split("-")
        if len(parts) != 2:
            print(f"❌ Invalid period format in filename: {filename}")
            return None
        start, end = parts
        try:
            start_int, end_int = int(start), int(end)
            if not (1 <= start_int <= 12) or not (1 <= end_int <= 12):
                print(f"❌ Month out of range in filename: {filename}")
                return None
            if start_int > end_int:
                print(f"❌ Start month > end month in filename: {filename}")
                return None
        except ValueError:
            print(f"❌ Non-numeric month in filename: {filename}")
            return None
    else:
        try:
            month_int = int(period)
            if not (1 <= month_int <= 12):
                print(f"❌ Month out of range in filename: {filename}")
                return None
        except ValueError:
            print(f"❌ Non-numeric month in filename: {filename}")
            return None

    return year, settlement, period


def main():
    """
    Main function: recursively scan all CSV files in CSV_IN_DIR and parse their names.
    """
    csv_dir = Path(CSV_IN_DIR)

    if not csv_dir.exists():
        print(f"❌ Directory {CSV_IN_DIR} does not exist!")
        return

    print(f"📂 Scanning directory: {csv_dir.absolute()}")

    for csv_file in csv_dir.rglob("*.csv"):
        rel_path = csv_file.relative_to(csv_dir)
        result = parse_filename(csv_file.name)

        if result:
            year, settlement, period = result
            print(f"✅ {rel_path} → Year: {year}, Settlement: {settlement}, Period: {period}")
        else:
            print(f"❌ Skipped: {rel_path}")


if __name__ == "__main__":
    main()