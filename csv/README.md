# CSV Analyser

A Python utility for quickly counting lines in large CSV files and performing field consistency analysis. Designed to stream input and avoid loading entire files into memory, making it suitable for very large datasets.

## Features

- Efficiently counts the number of lines in a CSV file
- Analyses field counts per line and reports inconsistencies
- Supports custom delimiters (default: comma)
- Optional skipping of header row
- Streams line-by-line to support large files
- Handles malformed rows gracefully

## Requirements

- Python 3.13+
- [uv](https://github.com/astral-sh/uv) for dependency and environment management

## Installation

```bash
uv venv .
source .venv/bin/activate
```

There are no external dependencies for the base functionality, as it uses only Python standard library modules.

## Usage

```bash
csv-analyser <path_to_file> [OPTIONS]
```

### Options

| Option            | Description                               |
|------------------|-------------------------------------------|
| `--analyse`       | Perform detailed field count analysis     |
| `--delimiter ","` | Specify custom CSV delimiter              |
| `--skip-header`   | Skip the first row during analysis        |

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
