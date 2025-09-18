import os
from pathlib import Path
from typing import Tuple, Optional, Dict
import re
import sys
import pandas as pd
from rapidfuzz import fuzz, process, distance
from dotenv import load_dotenv

# Load shared config first
load_dotenv("config.env")

# Override with local .env if exists
load_dotenv(".env", override=True)


CSV_IN_DIR = os.getenv("CSV_IN_DIR", "data/csv_in")
PARQUET_PATH = os.getenv("PARQUET_PATH", "data/data.parquet")
OUTPUTS_DIR = os.getenv("OUTPUTS_DIR", "outputs")
CSV_OUT_DIR = os.getenv("CSV_OUT_DIR", "data/csv_out")

# Ensure output directory exists
Path(OUTPUTS_DIR).mkdir(parents=True, exist_ok=True)


def load_mapping(csv_path: str, key_columns: list, value_column: str, name: str) -> Dict[str, str]:
    """
    Generic function to load mapping from CSV: maps values from key_columns to value_column.
    """
    if not Path(csv_path).exists():
        print(f"❌ {name} file not found at {csv_path}")
        sys.exit(1)

    df = pd.read_csv(csv_path, encoding="utf-8")
    required_cols = set(key_columns + [value_column])
    if not required_cols.issubset(df.columns):
        print(f"❌ {name} must contain columns: {required_cols}")
        sys.exit(1)

    mapping = {}
    for _, row in df.iterrows():
        value = row[value_column]
        for col in key_columns:
            key = row[col]
            if pd.notna(key):
                mapping[key] = value
    return mapping


# Load mappings
society = load_mapping("data/society.csv", ["Russian", "Synonym"], "English", "Society mapping")
credit_items = load_mapping("data/credit_items.csv", ["Russian"], "Name", "Credit items mapping")


def parse_filename(filename: str) -> Optional[Tuple[str, str, int, int]]:
    """
    Parse filename of format:
        "YEAR SETTLEMENT Month1-Month2.csv"
        "YEAR SETTLEMENT Month1.csv"
        "YEAR SETTLEMENT.csv"  → implies full year (1-12)

    Returns tuple (year, settlement_english, month_start, month_end) or None if failed.
    """
    if not filename.endswith(".csv"):
        return None

    stem = filename[:-4]  # Remove ".csv"

    pattern = r"^(\d{4})\s+(.+?)(?:\s+(\d{1,2}(?:-\d{1,2})?))?$"
    match = re.match(pattern, stem)

    if not match:
        print(f"⚠️  Failed to parse filename: {filename}")
        return None

    year_str, settlement_raw, period_str = match.groups()

    try:
        year = int(year_str)
        if not (1900 <= year <= 1999):
            print(f"❌ Invalid year in filename: {filename}")
            return None
    except ValueError:
        print(f"❌ Year is not an integer in filename: {filename}")
        return None

    if settlement_raw in society:
        settlement_english = society[settlement_raw]
    else:
        print(f"❌ Unknown settlement: '{settlement_raw}' in file '{filename}' — not found in society.csv")
        sys.exit(1)

    if period_str is None:
        month_start, month_end = 1, 12
    elif "-" in period_str:
        parts = period_str.split("-")
        if len(parts) != 2:
            print(f"❌ Invalid period format: {period_str} in {filename}")
            return None
        try:
            month_start = int(parts[0])
            month_end = int(parts[1])
        except ValueError:
            print(f"❌ Non-integer in period: {period_str} in {filename}")
            return None
    else:
        try:
            month = int(period_str)
            month_start = month_end = month
        except ValueError:
            print(f"❌ Single month not integer: {period_str} in {filename}")
            return None

    if not (1 <= month_start <= 12) or not (1 <= month_end <= 12):
        print(f"❌ Month out of range in: {filename}")
        return None

    return year, settlement_english, month_start, month_end


def clean_amount(amount_str: str) -> Optional[int]:
    """
    Extract integer from string like "25р." → 25
    Returns None if not possible.
    """
    if pd.isna(amount_str) or not isinstance(amount_str, str):
        return None
    # Remove "р." and any trailing/leading whitespace
    cleaned = amount_str.replace("р.", "").strip()
    try:
        return int(cleaned)
    except ValueError:
        return None



def process_csv_file(csv_path: Path, year: str, settlement: str, month_start: int, month_end: int) -> pd.DataFrame:
    """
    Read and process one CSV file, return cleaned DataFrame.
    Uses fuzzy matching to map "предмет выдачи" to credit_items.
    """
    df = pd.read_csv(csv_path, encoding="utf-8")

    # Skip "ИТОГО" row
    df = df[df["№"] != "ИТОГО"].copy()

    # Clean amount column
    df["на сумму (руб)"] = df["на сумму (руб)"].apply(clean_amount)

    # Filter rows where both loan_count and amount are not empty
    df = df.dropna(subset=["число ссуд", "на сумму (руб)"]).copy()

    # Load fuzzy threshold from .env
    fuzzy_threshold = int(os.getenv("FUZZY_THRESHOLD", "5"))

    # Map each "предмет выдачи" to closest match in credit_items
    unmatched_items = []
    matched_items = []

    for raw_item in df["предмет выдачи"]:
        # Find best match
        best_match, score, _ = process.extractOne(
            raw_item,
            credit_items.keys(),
            scorer=fuzz.QRatio,  # or fuzz.ratio — Levenshtein-based
            score_cutoff=100 - fuzzy_threshold * 10  # convert distance to similarity %
        )

        if best_match is None:
            unmatched_items.append(raw_item)
            matched_items.append(None)
        else:
            # Check Levenshtein distance explicitly
            dist = distance.Levenshtein.distance(raw_item, best_match)
            if dist <= fuzzy_threshold:
                matched_items.append(credit_items[best_match])
            else:
                unmatched_items.append(raw_item)
                matched_items.append(None)

    if unmatched_items:
        print(f"❌ Unmatched credit items in {csv_path.name} (distance > {fuzzy_threshold}):")
        for item in set(unmatched_items):  # unique values
            print(f"   → '{item}'")
        sys.exit(1)

    df["credit_item"] = matched_items

    # Select and rename columns
    result_df = df[["credit_item", "число ссуд", "на сумму (руб)", "более подробные сведения, на какие именно предметы выданы ссуды"]].copy()
    result_df.columns = ["credit_item", "loan_count", "amount_rubles", "loan_details"]

    # Add metadata
    result_df["year"] = year
    result_df["settlement"] = settlement
    result_df["month_start"] = month_start
    result_df["month_end"] = month_end

    return result_df


def main():
    csv_dir = Path(CSV_IN_DIR)
    if not csv_dir.exists():
        print(f"❌ Directory {CSV_IN_DIR} does not exist!")
        return

    all_data = []

    for csv_file in csv_dir.rglob("*.csv"):
        rel_path = csv_file.relative_to(csv_dir)
        result = parse_filename(csv_file.name)

        if not result:
            print(f"❌ Skipped: {rel_path}")
            continue

        year, settlement, month_start, month_end = result
        print(f"✅ Processing: {rel_path} → {settlement}, {year}, months {month_start}-{month_end}")

        try:
            df_clean = process_csv_file(csv_file, year, settlement, month_start, month_end)
            all_data.append(df_clean)
        except Exception as e:
            print(f"❌ Error processing {csv_file.name}: {e}")
            sys.exit(1)

    if not all_data:
        print("⚠️  No valid data to save.")
        return

    final_df = pd.concat(all_data, ignore_index=True)

    # Count unique settlements and credit items
    n_settlements = final_df["settlement"].nunique()
    n_items = final_df["credit_item"].nunique()

    # Generate output filename
    output_filename = f"loans_s{n_settlements}_i{n_items}.csv"
    output_path = Path(CSV_OUT_DIR) / output_filename

    # Reorder columns for clarity
    final_df = final_df[[
        "year", "settlement", "month_start", "month_end",
        "credit_item", "loan_count", "amount_rubles", "loan_details"
    ]]

    final_df.to_csv(output_path, index=False, encoding="utf-8")
    print(f"✅ Saved {len(final_df)} rows to {output_path}")

if __name__ == "__main__":
    main()