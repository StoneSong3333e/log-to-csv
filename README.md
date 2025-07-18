# Instrument Log Processor

A **configurable** tool to parse arbitrary instrument log files into a structured CSV, using a user-defined state machine.

## Features

- Configuration-driven: all paths, regex patterns, event sequence, and CSV columns live in `config.json`.
- Flexible state machine: default three steps (`init→config→execute`), but you can rename or add stages.
- Automatic encoding detection (via [chardet](https://pypi.org/project/chardet/)).
- Recursive file discovery, natural sort, and optional bulk rename.
- Robust error handling and detailed logging (`log_processor.log`).
- Easy to extend, test, and maintain.

## Installation

```bash
# clone this repo
git clone https://github.com/your-username/instrument-log-processor.git
cd instrument-log-processor

# (optional) create and activate a virtualenv
python3 -m venv .venv
source .venv/bin/activate

# install dependencies
pip install -r requirements.txt
```

## Usage

1. Edit `config.json` to point to your log folder, define event regexes, sequence, and desired CSV columns.
2. Run:

   ```bash
   python3 processor.py --config config.json
   ```

3. The CSV will be written to the path in `config.json` (or override with `-o output.csv`).

## Configuration

See `config.json` for a fully-commented example. Key sections:

- `FOLDER_PATH` / `OUTPUT_CSV` – input/output paths  
- `EVENT_SEQUENCE` – ordered list of your event stages  
- `PATTERN_CONFIG` – regex patterns for each stage  
- `CSV_FIELDS` – final CSV column order  

## Testing

```bash
pytest
```

## License

[MIT License](LICENSE)

## Disclaimer

This project is derived in part from internal company logic. It was developed outside of company time and equipment, and does not disclose any confidential or proprietary information. Use at your own risk; the author disclaims any liability for misuse.
