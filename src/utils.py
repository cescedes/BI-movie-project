import json
from pathlib import Path
import pandas as pd
from pandas._libs.missing import NAType


def ensure_directories(*dirs: Path) -> None:
    for directory in dirs:
        directory.mkdir(parents=True, exist_ok=True)

def unix_to_month_start(series: pd.Series) -> pd.Series:
    dt = pd.to_datetime(series, unit="s", utc=True)
    month_start = dt.dt.to_period("M").dt.to_timestamp()
    return pd.Series(month_start, index=series.index, name=series.name)

def parse_primary_genre(genres: str) -> str:
    if pd.isna(genres) or genres == "(no genres listed)":
        return "Unknown"
    return str(genres).split("|")[0].strip()

def safe_int_key(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").astype("Int64")

def imdb_numeric_to_tconst(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    return numeric.apply(
        lambda x: f"tt{int(x):07d}" if pd.notna(x) else pd.NA
    ).astype("string")

def first_pipe_value(value: str) -> str | NAType:
    if pd.isna(value) or value == r"\N" or str(value).strip() == "":
        return pd.NA
    return (
        str(value).split(",")[0].strip()
        if "," in str(value)
        else str(value).split("|")[0].strip()
    )

def normalize_imdb_nulls(df: pd.DataFrame) -> pd.DataFrame:
    return df.replace(r"\N", pd.NA)

def read_json(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def first_list_name(items) -> str | NAType:
    if not items or not isinstance(items, list):
        return pd.NA
    first = items[0]
    if isinstance(first, dict):
        return str(first.get("name", "")).strip() or pd.NA
    return str(first).strip() or pd.NA