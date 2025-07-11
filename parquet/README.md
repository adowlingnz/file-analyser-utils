# Parquet Analyser Utility

A command-line utility for analysing Parquet files, providing schema overview, row statistics, duplicate detection, and advanced row search features.

## Features

- Print schema and field overview
- Analyse per-row non-null field counts
- Identify rows with sparse data
- Show per-column null value percentages
- Print malformed row numbers and/or data
- Detect duplicate rows based on leading columns
- Display top, tail, or specific rows with context
- Search for rows matching specific column values

## Requirements

- Python 3.13+
- [pyarrow](https://pypi.org/project/pyarrow/)
- [uv](https://github.com/astral-sh/uv) (optional, for environment management)

## Usage

```sh
python parquet_analyser.py <file> [options]
```

### Options

- `--analyse`  
  Perform field count and schema analysis.

- `--top N`  
  Show the first N records.

- `--tail N`  
  Show the last N records.

- `--row N`  
  Show row N and N rows before/after (see `--context`).

- `--context N`  
  Number of rows before/after for `--row` (default: 5).

- `--print-malformed-rows`  
  Print row numbers of malformed rows (rows with unexpected field counts).

- `--print-malformed-data`  
  Print actual data of malformed rows.

- `--check-duplicates N`  
  Check for duplicate rows based on the first N columns.

- `--find '{"col1": value1, "col2": value2, ...}'`  
  Find and print rows matching the specified column values (pass as a JSON string).

### Examples

**Show schema and field overview:**
```sh
python parquet_analyser.py data.parquet
```

**Analyse file and print malformed row numbers:**
```sh
python parquet_analyser.py data.parquet --analyse --print-malformed-rows
```

**Show first 10 rows:**
```sh
python parquet_analyser.py data.parquet --top 10
```

**Show row 100 and 10 rows before/after:**
```sh
python parquet_analyser.py data.parquet --row 100 --context 10
```

**Check for duplicate rows based on first 3 columns:**
```sh
python parquet_analyser.py data.parquet --analyse --check-duplicates 3
```

**Find rows where x=7843.75, y=101.25, z=25:**
```sh
python parquet_analyser.py data.parquet --find '{"x":7843.75,"y":101.25,"z":25}'
```

## License

MIT License © 2025 Anthony Dowling
