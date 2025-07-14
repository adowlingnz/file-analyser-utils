import csv
import argparse
from collections import defaultdict
import json
import tomllib  # Built-in from Python 3.11+
import pandas as pd
from pathlib import Path
import time


def get_project_metadata(pyproject_path="pyproject.toml"):
    """Reads project metadata from pyproject.toml."""
    path = Path(pyproject_path)
    if not path.exists():
        return ("Unknown", "0.0.0")

    with open(path, "rb") as f:
        data = tomllib.load(f)
        name = data.get("project", {}).get("name", "Unknown")
        version = data.get("project", {}).get("version", "0.0.0")
        return (name, version)


def load_and_describe_csv(file_path):
    """Load a CSV file and print its details, row count, and column overview."""
    print("Loading CSV file...\n")
    print(f"  File: {file_path}\n")
    df = pd.read_csv(file_path)
    num_columns = len(df.columns)
    num_rows = len(df)
    print(f"  Columns: {num_columns}, Rows: {num_rows:,}")
    print("\n  Field Overview:\n")
    for i, col in enumerate(df.columns):
        print(f"    {i+1}. {col} ({df[col].dtype})")
    print()
    return df, num_columns, num_rows


def find_rows_by_column_values(file_path, column_values, raw=False):
    """
    Search for rows where the specified columns match the given values.
    If raw=True, search for the raw string in the CSV file.
    :param file_path: Path to the CSV file.
    :param column_values: Dict of {column_name: value} to match, or a raw string if raw=True.
    :param raw: If True, search for the raw string in the CSV file.
    """
    if raw:
        # If column_values is a dict, convert to a search string
        if isinstance(column_values, dict):
            search_str = ','.join(str(v) for v in column_values.values())
        else:
            search_str = str(column_values)
        print(f"\nSearching for raw string '{search_str}' in {file_path}...\n")
        matches = []
        with open(file_path, 'r', encoding='utf-8', newline='') as f:
            for idx, line in enumerate(f, start=1):
                if search_str in line:
                    print(f"Row {idx:,}: {line.rstrip()}\n")
                    matches.append(idx)
        if matches:
            print(f"\nFound {len(matches):,} matching row(s).")
        else:
            print("No matching rows found.")
    else:
        df = pd.read_csv(file_path)
        columns = list(column_values.keys())
        print(f"\nSearching for rows where {column_values}...\n")
        matches = df.loc[(df[columns] == pd.Series(column_values)).all(axis=1)]
        if matches.empty:
            print("No matching rows found.")
        else:
            for idx, row in matches.iterrows():
                print(f"Row {idx + 1:,}: {row.to_dict()}\n")
            print(f"\nFound {len(matches):,} matching row(s).")


def show_records(file_path, count, tail=False, raw=False):
    """Display the first or last N records from the CSV file."""
    df, _, num_rows = load_and_describe_csv(file_path)
    if count > num_rows:
        count = num_rows
    if tail:
        subset = df.tail(count)
        start = num_rows - count
    else:
        subset = df.head(count)
        start = 0
    print(f"\nDisplaying {'last' if tail else 'first'} {count} record(s):\n")
    for i, (_, row) in enumerate(subset.iterrows()):
        if raw:
            # Read raw line from file
            with open(file_path, 'r', encoding='utf-8', newline='') as f:
                for idx, line in enumerate(f):
                    if idx == start + i:
                        print(f"Row {start + i + 1:,}: {line.rstrip()}")
                        break
        else:
            print(f"Row {start + i + 1:,}: {row.to_dict()}")


def show_row_with_context(file_path, row_number, context=5, raw=False):
    """Display a specific row and N rows before/after it."""
    df, _, num_rows = load_and_describe_csv(file_path)
    row_number = max(1, min(row_number, num_rows))
    start = max(0, row_number - context - 1)
    end = min(num_rows, row_number + context)
    subset = df.iloc[start:end]
    print(f"\nDisplaying rows {start + 1:,} to {end:,} (row {row_number:,} highlighted):\n")
    for i, (_, row) in enumerate(subset.iterrows()):
        idx = start + i + 1
        prefix = "\n" if idx == row_number else ""
        suffix = "\n" if idx == row_number else ""
        if raw:
            with open(file_path, 'r', encoding='utf-8', newline='') as f:
                for line_idx, line in enumerate(f):
                    if line_idx == idx - 1:
                        print(f"{prefix}Row {idx:,}: {line.rstrip()}{suffix}")
                        break
        else:
            print(f"{prefix}Row {idx:,}: {row.to_dict()}{suffix}")


def analyse_csv(file_path, delimiter=',', skip_header=False):
    """
    Analyse a CSV file:
    - Count the number of fields in each row
    - Tally how many lines have each field count
    - Report lines that have differing field counts
    """
    field_count_summary = defaultdict(int)
    expected_field_count = None
    malformed_lines = []

    with open(file_path, 'r', encoding='utf-8', newline='') as file:
        reader = csv.reader(file, delimiter=delimiter)
        for line_number, row in enumerate(reader, start=1):
            if skip_header and line_number == 1:
                continue

            try:
                field_count = len(row)
                field_count_summary[field_count] += 1

                if expected_field_count is None:
                    expected_field_count = field_count
                elif field_count != expected_field_count:
                    malformed_lines.append(line_number)

            except Exception:
                print(f"Skipping malformed line {line_number:,}")
                continue

    print("Field Count Summary:")
    for count, num_lines in sorted(field_count_summary.items()):
        print(f"  {num_lines:,} line(s) with {count:,} field(s)")

    if malformed_lines:
        print("\nLines with inconsistent field counts:")
        for line in malformed_lines:
            print(f"  Line {line}")
    else:
        print("\nAll lines have consistent field counts.")


def compare_csv_files(file1, file2, check_header=True, check_data=True):
    """
    Compare two CSV files for equality.
    :param file1: Path to first CSV file.
    :param file2: Path to second CSV file.
    :param check_header: If True, compare column headers.
    :param check_data: If True, compare data content.
    """
    print(f"Comparing '{file1}' and '{file2}'...\n")
    df1 = pd.read_csv(file1)
    df2 = pd.read_csv(file2)

    same_shape = df1.shape == df2.shape

    if check_header:
        print("Checking column headers...\n")
        if list(df1.columns) == list(df2.columns):
            print("  Headers match.\n")
            same_header = True
        else:
            print("  Headers do NOT match.\n")
            print(f"  File1 columns: {list(df1.columns)}")
            print(f"  File2 columns: {list(df2.columns)}")
            same_header = False
    else:
        same_header = True

    if not same_shape:
        print(f"  Shape mismatch: {df1.shape} vs {df2.shape}")
        print("Files are NOT identical.")
        return False

    if check_data:
        print("Checking data...\n")
        if df1.equals(df2):
            print("  Data matches.")
            result = True
        else:
            print("  Data does NOT match.")
            result = False
    else:
        result = same_header and same_shape

    if result:
        print("\nFiles are IDENTICAL.")
    else:
        print("\nFiles are NOT identical.")
    return result


def main():
    parser = argparse.ArgumentParser(description="CSV File Analysis Utility")
    parser.add_argument("file", help="Path to the CSV file")
    parser.add_argument("--top", type=int, help="Show the first N records")
    parser.add_argument("--tail", type=int, help="Show the last N records")
    parser.add_argument("--row", type=int, help="Show a specific row and N either side")
    parser.add_argument("--context", type=int, default=5, help="Number of rows before/after for --row (default: 5)")
    parser.add_argument(
        "--raw",
        action="store_true",
        help=(
            "Display raw CSV lines instead of formatted output "
            "or search for raw string in --find"
        ),
    )
    parser.add_argument("--compare", type=str, metavar="SECOND_FILE", help="Compare the main file to a second CSV file")
    parser.add_argument("--find", type=str, help="Find rows matching column values (JSON string or raw string)")

    args = parser.parse_args()

    print("CSV Analyser starting...\n")
    print("Runtime Configuration:")
    print(f"  File: {args.file}")

    if args.compare:
        print(f"  Mode: Compare ({args.file} vs {args.compare})")
    elif args.find:
        print(f"  Mode: Find ({args.find})")
    elif args.row:
        print(f"  Mode: Row ({args.row} ± {args.context} records)")
    elif args.top:
        print(f"  Mode: Top ({args.top} records)")
    elif args.tail:
        print(f"  Mode: Tail ({args.tail} records)")
    else:
        print("  Mode: Describe")

    print()

    start = time.time()

    if args.compare:
        compare_csv_files(args.file, args.compare)
    elif args.find:
        try:
            column_values = json.loads(args.find)
            find_rows_by_column_values(args.file, column_values, raw=args.raw)
        except Exception:
            # If not JSON, treat as raw string search
            find_rows_by_column_values(args.file, args.find, raw=True)
    elif args.row:
        show_row_with_context(args.file, args.row, context=args.context, raw=args.raw)
    elif args.top:
        show_records(args.file, args.top, tail=False, raw=args.raw)
    elif args.tail:
        show_records(args.file, args.tail, tail=True, raw=args.raw)
    else:
        load_and_describe_csv(args.file)

    print(f"\nProcessing time: {time.time() - start:.2f} seconds")


if __name__ == "__main__":
    main()
