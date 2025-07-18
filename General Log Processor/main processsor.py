import os
import re
import json
import argparse
import logging
import sys
import csv
import chardet
from datetime import datetime
from collections import deque, defaultdict


class ConfigLoader:
    """
    Load and validate configuration from a JSON file, merge with defaults.
    """
    DEFAULT_CONFIG = {
        "FOLDER_PATH": "./logs",
        "OUTPUT_CSV": "./results/output.csv",
        "RENAME_LOGS": False,
        "NEW_EXT": ".txt",
        "LOG_EXTENSIONS": [".log", ".txt"],
        "TIMESTAMP_FORMATS": [
            "%Y/%m/%d %H:%M:%S.%f",
            "%Y-%m-%d %H:%M:%S.%f",
            "%d/%m/%Y %H:%M:%S.%f"
        ],
        "PATTERN_CONFIG": {},
        "EVENT_SEQUENCE": [],
        "CSV_FIELDS": [],
        "LOG_LEVEL": "INFO"
    }

    @staticmethod
    def load_config(config_path):
        """
        Read JSON configuration, merge with DEFAULT_CONFIG, and perform validation.
        Exits on critical errors.
        """
        try:
            with open(config_path, 'r') as f:
                user_cfg = json.load(f)
        except FileNotFoundError:
            logging.warning(f"Config file not found: {config_path}, using defaults.")
            user_cfg = {}
        except json.JSONDecodeError as e:
            logging.error(f"Invalid JSON in config: {e}")
            sys.exit(1)

        # Merge defaults and user configuration
        cfg = ConfigLoader.DEFAULT_CONFIG.copy()
        cfg.update(user_cfg)

        # Validate required configuration keys
        required_keys = [
            'FOLDER_PATH', 'OUTPUT_CSV',
            'EVENT_SEQUENCE', 'PATTERN_CONFIG',
            'CSV_FIELDS'
        ]
        for key in required_keys:
            if key not in cfg or cfg[key] is None:
                logging.error(f"Missing or empty config key: {key}")
                sys.exit(1)

        # Ensure each event in sequence has corresponding patterns
        for evt in cfg['EVENT_SEQUENCE']:
            if evt not in cfg['PATTERN_CONFIG']:
                logging.error(f"Event '{evt}' in sequence not in PATTERN_CONFIG")
                sys.exit(1)

        # Validate log folder exists
        if not os.path.isdir(cfg['FOLDER_PATH']):
            logging.error(f"Invalid FOLDER_PATH: {cfg['FOLDER_PATH']}")
            sys.exit(1)

        return cfg


class GenericLogProcessor:
    """
    Processes instrument log files into a CSV using a configurable state machine.
    """
    VERSION = "1.3.0"

    def __init__(self, config):
        self.config = config
        self.events = []            # collected events
        self.results = []           # finalized cycle records
        self.pending_cycles = deque()
        self.metrics = defaultdict(int)
        self.file_encodings = {}

        # Configure logging: console + file
        logging.basicConfig(
            level=getattr(logging, config['LOG_LEVEL']),
            format='%(asctime)s [%(levelname)s] %(message)s',
            handlers=[
                logging.FileHandler("log_processor.log"),
                logging.StreamHandler(sys.stdout)
            ]
        )
        logging.info(f"Initialized LogProcessor v{self.VERSION}")

    def process_files(self, output_csv=None):
        """
        Main pipeline:
          1. Optional rename logs
          2. Gather file paths
          3. Parse each file into events
          4. Analyze events into cycles
          5. Save CSV and print summary
        """
        if self.config['RENAME_LOGS']:
            self.rename_logs()

        file_paths = self.gather_files()
        if not file_paths:
            logging.error("No log files found.")
            return

        for path in file_paths:
            self.process_file(path)

        self.analyze_events()
        self.save_results(output_csv or self.config['OUTPUT_CSV'])
        self.print_summary()

    def rename_logs(self):
        """Rename raw log files by appending NEW_EXT."""
        folder = self.config['FOLDER_PATH']
        for fname in os.listdir(folder):
            if any(fname.endswith(ext) for ext in self.config['LOG_EXTENSIONS']):
                root, _ = os.path.splitext(fname)
                newname = root + self.config['NEW_EXT']
                os.rename(
                    os.path.join(folder, fname),
                    os.path.join(folder, newname)
                )
                logging.info(f"Renamed {fname} to {newname}")

    def gather_files(self):
        """Recursively collect log files matching LOG_EXTENSIONS, sorted naturally."""
        exts = self.config['LOG_EXTENSIONS']
        paths = []
        for root, _, files in os.walk(self.config['FOLDER_PATH']):
            for f in files:
                if any(f.endswith(ext) for ext in exts):
                    paths.append(os.path.join(root, f))
        paths.sort(key=self.natural_sort_key)
        logging.info(f"Found {len(paths)} log files")
        return paths

    @staticmethod
    def natural_sort_key(path):
        """Generate a key that sorts filenames with numeric parts logically."""
        base = os.path.basename(path)
        parts = re.split(r'(\d+)', base)
        return [int(p) if p.isdigit() else p.lower() for p in parts]

    def detect_encoding(self, path):
        """Detect and cache file encoding using chardet."""
        if path in self.file_encodings:
            return self.file_encodings[path]
        try:
            raw = open(path, 'rb').read()
            enc = chardet.detect(raw)['encoding'] or 'utf-8'
        except Exception:
            enc = 'utf-8'
        self.file_encodings[path] = enc
        return enc

    def process_file(self, path):
        """Parse each line of a file into events."""
        try:
            enc = self.detect_encoding(path)
            with open(path, 'r', encoding=enc, errors='replace') as f:
                for line in f:
                    self.parse_line(line, path)
            self.metrics['files_processed'] += 1
        except Exception as e:
            logging.error(f"Error processing {path}: {e}")

    def parse_line(self, line, filename):
        """Extract timestamp and event fields from a log line."""
        ts = self.extract_timestamp(line)
        if not ts:
            return
        for evt in self.config['EVENT_SEQUENCE']:
            patterns = self.config['PATTERN_CONFIG'][evt]
            matched = False
            event = {'time': ts, 'file': filename, 'type': evt}
            for field, pat in patterns.items():
                m = re.search(pat, line)
                if m:
                    matched = True
                    event[field] = m.group(1).strip()
            if matched:
                self.events.append(event)
                self.metrics['events_parsed'] += 1
                return
        logging.debug(f"No pattern matched: {line.strip()}")

    def extract_timestamp(self, line):
        """Parse timestamp in brackets using configured formats."""
        m = re.search(r"\[([^\]]+)\]", line)
        if not m:
            return None
        ts_str = m.group(1)
        for fmt in self.config['TIMESTAMP_FORMATS']:
            try:
                return datetime.strptime(ts_str, fmt)
            except ValueError:
                continue
        logging.debug(f"Unrecognized timestamp: {ts_str}")
        return None

    def analyze_events(self):
        """Group events into cycles based on EVENT_SEQUENCE state machine."""
        if not self.events:
            logging.warning("No events to analyze.")
            return
        seq = self.config['EVENT_SEQUENCE']
        self.events.sort(key=lambda e: e['time'])
        state = 0
        cycle = {evt: None for evt in seq}
        cycle[seq[-1]] = []  # last stage accumulates
        source_files = set()

        for ev in self.events:
            try:
                idx = seq.index(ev['type'])
            except ValueError:
                logging.debug(f"Unknown event type: {ev['type']}")
                continue
            if idx == 0:
                if state == len(seq):
                    self.finalize_cycle(cycle, source_files)
                state = 1
                cycle = {evt: None for evt in seq}
                cycle[seq[-1]] = []
                cycle[ev['type']] = ev
                source_files = {ev['file']}
            elif idx == state:
                if idx == len(seq) - 1:
                    cycle[ev['type']].append(ev)
                else:
                    cycle[ev['type']] = ev
                state += 1
                source_files.add(ev['file'])
            else:
                logging.warning(f"Unexpected event order: {ev['type']} at state {state}")
                state = 0

        if state == len(seq):
            self.finalize_cycle(cycle, source_files)

    def finalize_cycle(self, cycle, source_files):
        """Build and save a record if cycle is valid."""
        try:
            rec = self.build_record(cycle, source_files)
            if self.validate_record(rec):
                self.results.append(rec)
                self.metrics['cycles_completed'] += 1
        except Exception as e:
            logging.error(f"Error finalizing cycle: {e}")
            self.metrics['cycle_errors'] += 1

    def build_record(self, cycle, source_files):
        """Construct a CSV record from cycle data."""
        tech = cycle.get('technique')
        qry  = cycle.get('query')
        last_stage = self.config['EVENT_SEQUENCE'][-1]
        timing_events = cycle.get(last_stage, [])

        record = {
            'SourceFiles': '+'.join(sorted(source_files)),
            'StartTime': tech['time'].strftime("%y/%m/%d %H:%M:%S") if tech else 'NA'
        }
        # Merge technique and query fields
        for stage in ['query', 'technique']:
            evt = cycle.get(stage)
            if evt:
                for field in self.config['PATTERN_CONFIG'][stage]:
                    record[field] = evt.get(field, 'NA')
        # Calculate averages for timing fields
        for field in self.config['PATTERN_CONFIG'][last_stage]:
            values = []
            for e in timing_events:
                try:
                    values.append(float(e.get(field, 0)))
                except (ValueError, TypeError):
                    continue
            record[f'Avg{field}'] = round(sum(values) / len(values), 2) if values else None
        # Duration and frame count
        record['frames'] = len(timing_events)
        if timing_events:
            try:
                start_t = timing_events[0]['time']
                end_t = timing_events[-1]['time']
                record['Duration(s)'] = round((end_t - start_t).total_seconds(), 3)
            except Exception:
                record['Duration(s)'] = None
        return record

    def validate_record(self, rec):
        """Ensure record has essential fields and no NaN values."""
        if not rec.get('StartTime') or not isinstance(rec.get('frames'), int):
            return False
        for v in rec.values():
            if isinstance(v, float) and v != v:  # NaN check
                return False
        return True

    def save_results(self, output_path):
        """Write all records to the CSV file."""
        if not self.results:
            logging.warning("No results to save.")
            return
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=self.config['CSV_FIELDS'])
            writer.writeheader()
            writer.writerows(self.results)
        logging.info(f"Saved {len(self.results)} records to {output_path}")

    def print_summary(self):
        """Print a summary of processing metrics."""
        print("\n===== Processing Summary =====")
        print(f"Files processed:   {self.metrics['files_processed']}")
        print(f"Events parsed:     {self.metrics['events_parsed']}")
        print(f"Cycles completed:  {self.metrics['cycles_completed']}")
        if self.metrics.get('cycle_errors'):
            print(f"Cycle errors:      {self.metrics['cycle_errors']}")
        print(f"Records saved:     {len(self.results)}")


# ========== Main ==========
def main():
    parser = argparse.ArgumentParser(
        description='Process instrument logs into structured CSV.'
    )
    parser.add_argument(
        '-c', '--config', default='config.json',
        help='Path to JSON configuration file'
    )
    parser.add_argument(
        '-o', '--output',
        help='Override output CSV file path'
    )
    args = parser.parse_args()

    cfg = ConfigLoader.load_config(args.config)
    processor = GenericLogProcessor(cfg)
    processor.process_files(output_csv=args.output)


if __name__ == '__main__':
    main()
