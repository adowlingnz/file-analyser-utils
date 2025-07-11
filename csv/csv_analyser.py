import csv
import argparse
from collections import defaultdict
import tomllib  # Built-in from Python 3.11+
from pathlib import Path


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


def count_lines(file_path):
    """Count the total number of lines in the file."""
    line_count = 0
    with open(file_path, 'r', encoding='utf-8', newline='') as file:
        for _ in file:
            line_count += 1
    print(f"Total lines: {line_count:,}")


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


def main():
    parser = argparse.ArgumentParser(description="CSV File Analysis Utility")
    parser.add_argument("file", help="Path to the CSV file")
    parser.add_argument("--delimiter", default=",", help="CSV delimiter (default: ',')")
    parser.add_argument("--skip-header", action="store_true", help="Skip the first row (header)")
    parser.add_argument("--analyse", action="store_true", help="Perform field count analysis")

    args = parser.parse_args()

    name, version = get_project_metadata()

    print(f"{name} v{version} starting...\n")
    print("Configuration:")
    print(f"  File:           {args.file}")
    print(f"  Delimiter:      '{args.delimiter}'")
    print(f"  Skip Header:    {'Yes' if args.skip_header else 'No'}")
    print(f"  Mode:           {'Analysis' if args.analyse else 'Line Count'}\n")

    if args.analyse:
        analyse_csv(args.file, delimiter=args.delimiter, skip_header=args.skip_header)
    else:
        count_lines(args.file)


if __name__ == "__main__":
    main()
