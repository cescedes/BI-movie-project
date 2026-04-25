""" 
from the project root, run with:
python src/phase4_tmdb_enrichment.py

it should:
- cache raw TMDb JSON files in data/raw/tmdb/details/
- create a flat staging file at data/staging/tmdb_movies.csv
- update data/exports/dim_movie.csv
"""

import os
import time
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv
from tqdm import tqdm

from config import (
    DIM_MOVIE_PATH,
    STAGING_TMDB_MOVIES_PATH,
    TMDB_DETAILS_JSON_DIR,
    TMDB_BASE_URL,
)
from utils import ensure_directories, read_json, write_json


REQUEST_SLEEP_SECONDS = 0.20
FORCE_REFRESH = False


def load_dim_movie() -> pd.DataFrame:
    return pd.read_csv(DIM_MOVIE_PATH)


def load_api_key() -> str:
    load_dotenv()
    api_key = os.getenv("TMDB_API_KEY")
    if not api_key:
        raise ValueError("TMDB_API_KEY not found in .env")
    return api_key


def clean_tmdb_ids(dim_movie: pd.DataFrame) -> pd.DataFrame:
    df = dim_movie.copy()
    df["tmdbId"] = pd.to_numeric(df["tmdbId"], errors="coerce").astype("Int64")
    df = df[df["tmdbId"].notna()].copy()
    return df


def detail_json_path(tmdb_id: int) -> Path:
    return TMDB_DETAILS_JSON_DIR / f"{tmdb_id}.json"


def fetch_tmdb_movie_details(tmdb_id: int, api_key: str) -> dict:
    url = f"{TMDB_BASE_URL}/movie/{tmdb_id}"
    params = {
        "api_key": api_key,
        "language": "en-US",
    }

    response = requests.get(url, params=params, timeout=30)

    if response.status_code == 404:
        return {"tmdb_id": tmdb_id, "fetch_status": "not_found"}

    response.raise_for_status()

    payload = response.json()
    payload["tmdb_id"] = tmdb_id
    payload["fetch_status"] = "ok"
    return payload


def fetch_or_load_details(tmdb_id: int, api_key: str, force_refresh: bool = False) -> dict:
    path = detail_json_path(tmdb_id)

    if path.exists() and not force_refresh:
        payload = read_json(path)
        if "tmdb_id" not in payload:
            payload["tmdb_id"] = tmdb_id
        if "fetch_status" not in payload:
            payload["fetch_status"] = "ok"
        return payload

    payload = fetch_tmdb_movie_details(tmdb_id, api_key)
    write_json(path, payload)
    time.sleep(REQUEST_SLEEP_SECONDS)
    return payload


def flatten_tmdb_payload(payload: dict) -> dict:
    companies = payload.get("production_companies", [])
    countries = payload.get("production_countries", [])

    primary_company = companies[0]["name"] if companies and isinstance(companies[0], dict) else pd.NA
    primary_country = countries[0]["name"] if countries and isinstance(countries[0], dict) else pd.NA

    return {
        "tmdb_id": payload.get("tmdb_id"),
        "tmdb_fetch_status": payload.get("fetch_status"),
        "tmdb_title": payload.get("title"),
        "tmdb_release_date": payload.get("release_date"),
        "tmdb_original_language": payload.get("original_language"),
        "tmdb_popularity": payload.get("popularity"),
        "tmdb_budget": payload.get("budget"),
        "tmdb_revenue": payload.get("revenue"),
        "tmdb_status": payload.get("status"),
        "tmdb_vote_average": payload.get("vote_average"),
        "tmdb_vote_count": payload.get("vote_count"),
        "tmdb_primary_company": primary_company,
        "tmdb_primary_country": primary_country,
    }


def fetch_all_tmdb_details(dim_movie_tmdb: pd.DataFrame, api_key: str) -> pd.DataFrame:
    unique_tmdb_ids = (
        dim_movie_tmdb["tmdbId"]
        .dropna()
        .astype("Int64")
        .drop_duplicates()
        .tolist()
    )

    rows = []
    for tmdb_id in tqdm(unique_tmdb_ids, desc="Fetching TMDb details"):
        payload = fetch_or_load_details(int(tmdb_id), api_key, force_refresh=FORCE_REFRESH)
        rows.append(flatten_tmdb_payload(payload))

    tmdb_movies = pd.DataFrame(rows)

    if not tmdb_movies.empty:
        tmdb_movies["tmdb_id"] = pd.to_numeric(tmdb_movies["tmdb_id"], errors="coerce").astype("Int64")
        tmdb_movies["tmdb_popularity"] = pd.to_numeric(tmdb_movies["tmdb_popularity"], errors="coerce")
        tmdb_movies["tmdb_budget"] = pd.to_numeric(tmdb_movies["tmdb_budget"], errors="coerce").astype("Int64")
        tmdb_movies["tmdb_revenue"] = pd.to_numeric(tmdb_movies["tmdb_revenue"], errors="coerce").astype("Int64")
        tmdb_movies["tmdb_vote_average"] = pd.to_numeric(tmdb_movies["tmdb_vote_average"], errors="coerce")
        tmdb_movies["tmdb_vote_count"] = pd.to_numeric(tmdb_movies["tmdb_vote_count"], errors="coerce").astype("Int64")

    return tmdb_movies


def merge_tmdb_into_dim_movie(dim_movie: pd.DataFrame, tmdb_movies: pd.DataFrame) -> pd.DataFrame:
    df = dim_movie.copy()
    df["tmdbId"] = pd.to_numeric(df["tmdbId"], errors="coerce").astype("Int64")

    tmdb_movies = tmdb_movies.drop_duplicates(subset=["tmdb_id"]).copy()

    enriched = df.merge(
        tmdb_movies,
        left_on="tmdbId",
        right_on="tmdb_id",
        how="left",
    )

    if "release_year" in enriched.columns and "tmdb_release_date" in enriched.columns:
        tmdb_release_year = pd.to_datetime(enriched["tmdb_release_date"], errors="coerce").dt.year.astype("Int64")
        enriched["release_year"] = enriched["release_year"].fillna(tmdb_release_year)

    return enriched


def quality_report(enriched: pd.DataFrame) -> None:
    total = len(enriched)
    matched = enriched["tmdb_fetch_status"].eq("ok").sum() if "tmdb_fetch_status" in enriched.columns else 0
    has_language = enriched["tmdb_original_language"].notna().sum() if "tmdb_original_language" in enriched.columns else 0
    has_company = enriched["tmdb_primary_company"].notna().sum() if "tmdb_primary_company" in enriched.columns else 0
    failed_fetches = enriched["tmdb_fetch_status"].eq("not_found").sum() if "tmdb_fetch_status" in enriched.columns else 0
    
    print("\nTMDb enrichment report")
    print("-" * 30)
    print(f"Rows in dim_movie: {total:,}")
    print(f"Matched TMDb details: {matched:,} ({matched / total:.1%})" if total else "Matched TMDb details: 0")
    print(f"Rows with language:   {has_language:,} ({has_language / total:.1%})" if total else "Rows with language: 0")
    print(f"Rows with company:    {has_company:,} ({has_company / total:.1%})" if total else "Rows with company: 0")
    print(f"Rows with TMDb not_found: {failed_fetches:,} ({failed_fetches / total:.1%})" if total else "Rows with TMDb not_found: 0")

def main() -> None:
    ensure_directories(TMDB_DETAILS_JSON_DIR, STAGING_TMDB_MOVIES_PATH.parent)

    api_key = load_api_key()
    dim_movie = load_dim_movie()
    dim_movie_tmdb = clean_tmdb_ids(dim_movie)

    tmdb_movies = fetch_all_tmdb_details(dim_movie_tmdb, api_key)
    tmdb_movies.to_csv(STAGING_TMDB_MOVIES_PATH, index=False)

    enriched = merge_tmdb_into_dim_movie(dim_movie, tmdb_movies)
    enriched.to_csv(DIM_MOVIE_PATH, index=False)

    quality_report(enriched)
    print(f"\nStaging TMDb file written to: {STAGING_TMDB_MOVIES_PATH}")
    print(f"Updated dim_movie written to: {DIM_MOVIE_PATH}")


if __name__ == "__main__":
    main()