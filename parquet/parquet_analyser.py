import argparse
from collections import defaultdict
import json
import tomllib  # Built-in from Python 3.11+
import pandas as pd
import numpy as np
from pathlib import Path
import pyarrow.parquet as pq
import time


def get_project_metadata(pyproject_path="pyproject.toml"):
    path = Path(pyproject_path)
    if not path.exists():
        return ("Unknown", "0.0.0")
    with open(path, "rb") as f:
        data = tomllib.load(f)
        name = data.get("project", {}).get("name", "Unknown")
        version = data.get("project", {}).get("version", "0.0.0")
        return (name, version)


def load_and_describe_parquet(file_path):
    """Load a Parquet file and print its details, row count, and schema overview."""
    print("Loading Parquet file...\n")
    print(f"  File: {file_path}\n")
    table = pq.read_table(file_path)
    schema = table.schema
    num_columns = len(schema.names)
    num_rows = table.num_rows

    # Get Parquet file metadata
    parquet_file = pq.ParquetFile(file_path)
    metadata = parquet_file.metadata
    created_by = metadata.created_by if hasattr(metadata, "created_by") else "Unknown"
    format_version = f"{metadata.format_version}" if hasattr(metadata, "format_version") else "Unknown"
    # Parquet v1 files have format_version == 1.0, v2 files have 2.0
    version_str = "v2" if "2" in format_version else "v1"

    print(f"  Schema: {num_columns} columns, {num_rows:,} rows")
    print(f"  Writer library: {created_by}")
    print(f"  Parquet format version: {format_version} ({version_str})")
    print("\n  Field Overview:\n")
    for i, field in enumerate(schema):
        print(f"    {i+1}. {field.name} ({field.type})")
    print()
    return table, schema, num_columns, num_rows


def analyse_parquet(file_path, print_malformed_rows=False, print_malformed_data=False, check_duplicates=None):
    """
    Analyse a Parquet file for:
    - Schema consistency (always expected in Parquet)
    - Per-row non-null field count summary
    - Rows with unexpectedly sparse data
    - Per-column null value percentages
    - Optionally print malformed row numbers and/or data
    - Optionally check for duplicate rows based on leading columns
    """
    table, schema, num_columns, num_rows = load_and_describe_parquet(file_path)

    print("Analysing Parquet file...\n")

    non_null_field_count_summary = defaultdict(int)
    sparse_rows = []
    column_null_counts = {name: 0 for name in schema.names}
    malformed_rows = []
    malformed_data = []
    duplicate_keys = defaultdict(list)

    progress_interval = max(1, num_rows // 100)

    for row_idx in range(num_rows):
        row = table.slice(row_idx, 1).to_pydict()
        non_null_fields = sum(1 for col in schema.names if row[col][0] is not None)

        for col in schema.names:
            if row[col][0] is None:
                column_null_counts[col] += 1

        non_null_field_count_summary[non_null_fields] += 1

        # Malformed: not the expected number of fields
        if non_null_fields != num_columns:
            malformed_rows.append(row_idx + 1)
            if print_malformed_data:
                malformed_data.append({col: row[col][0] for col in schema.names})

        # Flag rows where most fields are null
        if non_null_fields < num_columns * 0.5:
            sparse_rows.append(row_idx + 1)

        # Duplicate check
        if check_duplicates is not None and check_duplicates > 0:
            key = tuple(row[schema.names[i]][0] for i in range(min(check_duplicates, num_columns)))
            duplicate_keys[key].append(row_idx + 1)

        if (row_idx + 1) % progress_interval == 0 or (row_idx + 1) == num_rows:
            print(f"Progress: {row_idx + 1:,}/{num_rows:,} rows analysed...", end="\r")

    print("\n\nNon-Null Field Count Summary:")
    for count, rows in sorted(non_null_field_count_summary.items()):
        print(f"  {rows:,} row(s) with {count:,} non-null field(s)")

    if sparse_rows:
        print(f"\n {len(sparse_rows):,} row(s) have <50% non-null fields:")
        print("   ", ", ".join(str(r) for r in sparse_rows[:10]), end='')
        if len(sparse_rows) > 10:
            print(" ...")
        else:
            print()
    else:
        print("\nAll rows have at least 50% of fields populated.")

    print("\nColumn Null Percentage:")
    for col, nulls in column_null_counts.items():
        percent = (nulls / num_rows) * 100
        print(f"  {col:30}: {percent:6.2f}% null")

    if print_malformed_rows and malformed_rows:
        print(f"\nMalformed row numbers ({len(malformed_rows)}):")
        print(", ".join(str(r) for r in malformed_rows))

    if print_malformed_data and malformed_data:
        print(f"\nMalformed row data ({len(malformed_data)}):")
        for idx, data in zip(malformed_rows, malformed_data):
            print(f"Row {idx}: {data}")

    # Print duplicate rows based on leading columns
    if check_duplicates is not None and check_duplicates > 0:
        duplicates = {k: v for k, v in duplicate_keys.items() if len(v) > 1}
        if duplicates:
            print(f"\nDuplicate rows found based on first {check_duplicates} columns:")
            for key, rows in duplicates.items():
                formatted_rows = ', '.join(f"{r:,}" for r in rows)
                print(f"  Key {key}: rows {formatted_rows}")
        else:
            print(f"\nNo duplicate rows found based on first {check_duplicates} columns.")

    print("\nAnalysis complete.")


def find_rows_by_column_values(file_path, column_values):
    """
    Search for rows where the specified columns match the given values.
    :param file_path: Path to the Parquet file.
    :param column_values: Dict of {column_name: value} to match.
    """
    table, schema, num_columns, num_rows = load_and_describe_parquet(file_path)
    columns = list(column_values.keys())
    matches = []

    print(f"\nSearching for rows where {column_values}...\n")
    progress_interval = max(1, num_rows // 100)

    for row_idx in range(num_rows):
        row = table.slice(row_idx, 1).to_pydict()
        if all(row.get(col, [None])[0] == column_values[col] for col in columns):
            matches.append({col: row[col][0] for col in schema.names})
            print(f"Row {row_idx + 1:,}: {matches[-1]}\n")
        if (row_idx + 1) % progress_interval == 0 or (row_idx + 1) == num_rows:
            print(f"Progress: {row_idx + 1:,}/{num_rows:,} rows searched...", end="\r")

    print()  # Ensure a new line after progress

    if not matches:
        print("No matching rows found.")
    else:
        print(f"\nFound {len(matches):,} matching row(s).")


def show_records(file_path, count, tail=False):
    """Display the first or last N records from the Parquet file."""
    table, _, _, num_rows = load_and_describe_parquet(file_path)

    if count > num_rows:
        count = num_rows
    if tail:
        start = num_rows - count
        end = num_rows
    else:
        start = 0
        end = count
    subset = table.slice(start, end - start).to_pydict()
    print(f"\nDisplaying {'last' if tail else 'first'} {count} record(s):\n")
    for i in range(end - start):
        row = {col: subset[col][i] for col in table.schema.names}
        print(f"Row {start + i + 1}: {row}")


def show_row_with_context(file_path, row_number, context=5):
    """Display a specific row and N rows before/after it."""
    table, _, _, num_rows = load_and_describe_parquet(file_path)
    # Clamp row_number to valid range (1-based index)
    row_number = max(1, min(row_number, num_rows))
    start = max(0, row_number - context - 1)
    end = min(num_rows, row_number + context)
    subset = table.slice(start, end - start).to_pydict()
    print(f"\nDisplaying rows {start + 1:,} to {end:,} (row {row_number:,} highlighted):\n")
    for i in range(end - start):
        row = {col: subset[col][i] for col in table.schema.names}
        prefix = "\n" if (start + i + 1) == row_number else ""
        suffix = "\n" if (start + i + 1) == row_number else ""
        print(f"{prefix}Row {start + i + 1:,}: {row}{suffix}")


def schemas_equal(schema1, schema2):
    """Deeply compare two pyarrow schemas for field names and types, logging mismatches."""
    if len(schema1) != len(schema2):
        print(f"Schema length mismatch: {len(schema1)} vs {len(schema2)}")
        return False
    match = True
    for idx, (f1, f2) in enumerate(zip(schema1, schema2), 1):
        if f1.name != f2.name:
            print(f"  Field {idx} name mismatch: '{f1.name}' vs '{f2.name}'")
            match = False
        if f1.type != f2.type:
            print(f"  Field {idx} type mismatch for '{f1.name}': {f1.type} vs {f2.type}")
            match = False
    return match


def dataframe_equal_fuzzy(df1, df2, rtol=1e-6, atol=1e-8):
    if not df1.columns.equals(df2.columns):
        print("  Column order or names differ.")
        return False

    differences = {}
    for col in df1.columns:
        s1 = df1[col]
        s2 = df2[col]

        # Align dtypes for numeric comparison
        if pd.api.types.is_numeric_dtype(s1) and pd.api.types.is_numeric_dtype(s2):
            try:
                s1f = pd.to_numeric(s1, errors='coerce')
                s2f = pd.to_numeric(s2, errors='coerce')
                if not np.allclose(s1f, s2f, rtol=rtol, atol=atol, equal_nan=True):
                    differences[col] = (s1f != s2f).sum()
            except Exception:
                differences[col] = "Coercion failed"
        else:
            if not s1.equals(s2):
                differences[col] = (s1 != s2).sum()

    if differences:
        print("  Differences found in columns:")
        for col, count in differences.items():
            print(f"    {col}: {count} differing rows")
        return False
    return True


def compare_parquet_files(file1, file2, check_schema=True, check_data=True, fuzzy_data=True, rtol=1e-6, atol=1e-8):
    """
    Compare two Parquet files for equality.
    :param file1: Path to first Parquet file.
    :param file2: Path to second Parquet file.
    :param check_schema: If True, compare schemas.
    :param check_data: If True, compare data content.
    :param fuzzy_data: If True, use fuzzy comparison for numeric columns.
    :param rtol: Relative tolerance for numeric comparison.
    :param atol: Absolute tolerance for numeric comparison.
    """
    print(f"Comparing '{file1}' and '{file2}'...\n")
    table1, schema1, num_columns1, num_rows1 = load_and_describe_parquet(file1)
    table2, schema2, num_columns2, num_rows2 = load_and_describe_parquet(file2)

    same_shape = (num_columns1 == num_columns2) and (num_rows1 == num_rows2)

    if check_schema:
        print("Checking schema...\n")

        same_schema = schemas_equal(schema1, schema2)

        print(f"\n  Schemas {'match' if same_schema else 'do NOT match'}.\n")

    if not same_shape:
        print(f"  Shape mismatch: {num_columns1}x{num_rows1} vs {num_columns2}x{num_rows2}")
        print("Files are NOT identical.")
        return False

    if check_data:
        print("Checking data...\n")
        df1 = table1.to_pandas()
        df2 = table2.to_pandas()
        if fuzzy_data:
            result = dataframe_equal_fuzzy(df1, df2, rtol=rtol, atol=atol)
        else:
            result = df1.equals(df2)
            if not result:
                print("  Exact data comparison failed.")
    else:
        result = same_schema and same_shape

    if result:
        print("\nFiles are IDENTICAL.")
    else:
        print("\nFiles are NOT identical.")
    return result


def main():
    parser = argparse.ArgumentParser(description="Parquet File Analysis Utility")
    parser.add_argument("file", help="Path to the Parquet file")
    parser.add_argument("--analyse", action="store_true", help="Perform field count analysis")
    parser.add_argument("--top", type=int, help="Show the first N records")
    parser.add_argument("--tail", type=int, help="Show the last N records")
    parser.add_argument("--row", type=int, help="Show a specific row and N either side")
    parser.add_argument("--context", type=int, default=5, help="Number of rows before/after for --row (default: 5)")
    parser.add_argument("--print-malformed-rows", action="store_true", help="Print row numbers of malformed rows")
    parser.add_argument("--print-malformed-data", action="store_true", help="Print actual data of malformed rows")
    parser.add_argument("--check-duplicates", type=int, help="Check for duplicate rows based on first N columns")
    parser.add_argument("--find", type=str, help="Find rows matching column values (JSON string)")
    parser.add_argument("--compare", type=str, metavar="SECOND_FILE", help="Compare the main file to a second file")

    args = parser.parse_args()

    name, version = get_project_metadata()

    print(f"{name} v{version} starting...\n")
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
    elif args.analyse:
        print("  Mode: Analysis")
        if args.print_malformed_rows:
            print("  Option: Print malformed row numbers")
        if args.print_malformed_data:
            print("  Option: Print malformed row data")
        if args.check_duplicates:
            print(f"  Option: Check duplicates on first {args.check_duplicates} columns")
    else:
        print("  Mode: Describe")

    print()

    start = time.time()

    if args.compare:
        compare_parquet_files(args.file, args.compare)
    elif args.find:
        try:
            column_values = json.loads(args.find)
            find_rows_by_column_values(args.file, column_values)
        except Exception as e:
            print(f"Error parsing --find argument: {e}")
    elif args.row:
        show_row_with_context(args.file, args.row, context=args.context)
    elif args.top:
        show_records(args.file, args.top, tail=False)
    elif args.tail:
        show_records(args.file, args.tail, tail=True)
    elif args.analyse:
        analyse_parquet(
            args.file,
            print_malformed_rows=args.print_malformed_rows,
            print_malformed_data=args.print_malformed_data,
            check_duplicates=args.check_duplicates
        )
    else:
        load_and_describe_parquet(args.file)

    print(f"\nProcessing time: {time.time() - start:.2f} seconds")


if __name__ == "__main__":
    main()
