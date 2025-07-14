# CSV Analyser

A Python utility for quickly counting lines in CSV files and performing field consistency analysis. Uses pandas for top/tail/row/context features, which **does load the entire CSV file into memory**.

## Features

- Efficiently counts the number of lines in a CSV file
- Analyses field counts per line and reports inconsistencies
- Supports custom delimiters (default: comma)
- Optional skipping of header row
- Handles malformed rows gracefully
- **Display top, tail, or specific rows with context (like Parquet analyser)**
- **Display raw CSV lines with the `--raw` option for top, tail, and row/context views**

## Requirements

- Python 3.13+
- [uv](https://github.com/astral-sh/uv) for dependency and environment management
- [pandas](https://pypi.org/project/pandas/) (required for top/tail/row/context features)

## Installation

```bash
uv venv .
source .venv/bin/activate
uv pip install pandas
```

## Usage

```bash
csv-analyser <path_to_file> [OPTIONS]
```

### Options

| Option            | Description                                                        |
|-------------------|--------------------------------------------------------------------|
| `--analyse`       | Perform detailed field count analysis                              |
| `--delimiter ","` | Specify custom CSV delimiter                                       |
| `--skip-header`   | Skip the first row during analysis                                 |
| `--top N`         | Show the first N records                                           |
| `--tail N`        | Show the last N records                                            |
| `--row N`         | Show row N and N rows before/after (see `--context`)               |
| `--context N`     | Number of rows before/after for `--row` (default: 5)               |
| `--raw`           | Display raw CSV lines instead of formatted output                  |

### Examples

Count the number of lines in a CSV file:

```bash
csv-analyser data.csv
```

Analyse the CSV for field consistency, assuming it has a header row:

```bash
csv-analyser data.csv --analyse --skip-header
```

Analyse a semicolon-delimited CSV file:

```bash
csv-analyser data.csv --analyse --delimiter=";"
```

Show the first 10 rows:

```bash
csv-analyser data.csv --top 10
```

Show the last 10 rows:

```bash
csv-analyser data.csv --tail 10
```

Show row 100 and 5 rows before/after:

```bash
csv-analyser data.csv --row 100 --context 5
```

Show the first 10 rows as raw CSV lines:

```bash
csv-analyser data.csv --top 10 --raw
```

Show row 100 and 5 rows before/after as raw CSV lines:

```bash
csv-analyser data.csv --row 100 --context 5 --raw
```

## Project Structure

```
csv-analyser/
├── csv_analyser.py      # Main utility script
├── pyproject.toml       # Project metadata and configuration
└── README.md            # This documentation
```

## License

MIT License

## Author

Anthony Dowling
