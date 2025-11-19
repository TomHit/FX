# /opt/xauapi/api/utils/config_loader.py
import os, pathlib, yaml

CFG_DIR = pathlib.Path(os.getenv("XTL_CFG_DIR", "/opt/xauapi/api/configs/model"))

def load_yaml(name: str):
    with open(CFG_DIR / name, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

FEATURES = load_yaml("features.yaml")
TARGETS  = load_yaml("targets.yaml")
METRICS  = load_yaml("metrics.yaml")
