import os
import json
import pytest
from datetime import datetime
from processor import ConfigLoader, GenericLogProcessor

@pytest.fixture
def tmp_config(tmp_path):
    cfg = {
        "FOLDER_PATH": str(tmp_path / "logs"),
        "OUTPUT_CSV": str(tmp_path / "out.csv"),
        "RENAME_LOGS": False,
        "NEW_EXT": ".txt",
        "LOG_EXTENSIONS": [".log"],
        "TIMESTAMP_FORMATS": ["%Y/%m/%d %H:%M:%S.%f"],
        "EVENT_SEQUENCE": ["init", "execute"],
        "PATTERN_CONFIG": {
            "init": {"Step": r"Step=(\\d+)"},
            "execute": {"Value": r"Value=(\\d+)"}
        },
        "CSV_FIELDS": ["SourceFiles","StartTime","Step","AvgValue","Duration(s)","frames"],
        "LOG_LEVEL": "DEBUG"
    }
    # 写入配置
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(cfg))
    # 创建日志目录与样本
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    sample = log_dir / "sample.log"
    sample.write_text(
        "[2025/07/09 00:00:00.000] Step=1\n"
        "[2025/07/09 00:00:01.000] Value=100\n"
    )
    return str(config_path), cfg

def test_load_config(tmp_config):
    cfg_path, cfg = tmp_config
    loaded = ConfigLoader.load_config(cfg_path)
    assert loaded["FOLDER_PATH"] == cfg["FOLDER_PATH"]
    assert loaded["EVENT_SEQUENCE"] == cfg["EVENT_SEQUENCE"]

def test_full_cycle(tmp_config):
    cfg_path, cfg = tmp_config
    loaded = ConfigLoader.load_config(cfg_path)
    proc = GenericLogProcessor(loaded)
    # 指定 config.json 会让 processor 自动处理
    proc.process_files()
    assert proc.metrics["files_processed"] == 1
    assert proc.metrics["events_parsed"] == 2
    assert proc.metrics["cycles_completed"] == 1
    rec = proc.results[0]
    assert rec["Step"] == "1"
    assert rec["AvgValue"] == 100.0
    assert rec["frames"] == 1
