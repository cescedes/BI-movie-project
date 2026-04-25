""" 
from the project root, run with:
python src/phase5_finalize_exports.py

exports a frozen final dataset in data/final/ that is ready for Tableau.
"""

import pandas as pd

from config import (
    DIM_DATE_PATH,
    DIM_MOVIE_PATH,
    FACT_MOVIE_MONTH_PATH,
    FINAL_DIR,
    FINAL_DIM_DATE_PATH,
    FINAL_DIM_MOVIE_PATH,
    FINAL_DIM_GENRE_PATH,
    FINAL_FACT_MOVIE_MONTH_PATH,
)
from utils import ensure_directories


def load_exports() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    dim_date = pd.read_csv(DIM_DATE_PATH)
    dim_movie = pd.read_csv(DIM_MOVIE_PATH)
    fact_movie_month = pd.read_csv(FACT_MOVIE_MONTH_PATH)
    return dim_date, dim_movie, fact_movie_month


def standardize_dim_movie(dim_movie: pd.DataFrame) -> pd.DataFrame:
    df = dim_movie.copy()

    # Keep the most useful columns for analysis
    preferred_cols = [
        "movie_key",
        "movie_id",
        "title",
        "primary_genre",
        "genres",
        "release_year",
        "imdbId",
        "imdb_tconst",
        "tmdbId",
        "title_type",
        "is_adult",
        "runtime_minutes",
        "imdb_start_year",
        "imdb_genres",
        "imdb_avg_rating",
        "imdb_num_votes",
        "director_name",
        "effective_genres",
        "tmdb_title",
        "tmdb_release_date",
        "tmdb_original_language",
        "tmdb_popularity",
        "tmdb_budget",
        "tmdb_revenue",
        "tmdb_status",
        "tmdb_vote_average",
        "tmdb_vote_count",
        "tmdb_primary_company",
        "tmdb_primary_country",
    ]

    existing_cols = [c for c in preferred_cols if c in df.columns]
    df = df[existing_cols].copy()

    # Ensure keys are numeric where appropriate
    if "movie_key" in df.columns:
        df["movie_key"] = pd.to_numeric(df["movie_key"], errors="coerce").astype("Int64")

    if "movie_id" in df.columns:
        df["movie_id"] = pd.to_numeric(df["movie_id"], errors="coerce").astype("Int64")

    if "release_year" in df.columns:
        df["release_year"] = pd.to_numeric(df["release_year"], errors="coerce").astype("Int64")

    if "runtime_minutes" in df.columns:
        df["runtime_minutes"] = pd.to_numeric(df["runtime_minutes"], errors="coerce").astype("Int64")

    df = df.sort_values("movie_key").drop_duplicates(subset=["movie_key"]).reset_index(drop=True)
    return df


def standardize_dim_date(dim_date: pd.DataFrame) -> pd.DataFrame:
    df = dim_date.copy()

    preferred_cols = [
        "month_key",
        "month_start",
        "year",
        "month_number",
        "month_name",
        "quarter",
        "year_month",
        "decade",
    ]
    existing_cols = [c for c in preferred_cols if c in df.columns]
    df = df[existing_cols].copy()

    df["month_key"] = pd.to_numeric(df["month_key"], errors="coerce").astype("Int64")
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    df["month_number"] = pd.to_numeric(df["month_number"], errors="coerce").astype("Int64")
    df["decade"] = pd.to_numeric(df["decade"], errors="coerce").astype("Int64")

    df = df.sort_values("month_key").drop_duplicates(subset=["month_key"]).reset_index(drop=True)
    return df


def standardize_fact_movie_month(fact_movie_month: pd.DataFrame) -> pd.DataFrame:
    df = fact_movie_month.copy()

    preferred_cols = [
        "movie_key",
        "month_key",
        "rating_count",
        "avg_rating",
        "tag_count",
    ]
    existing_cols = [c for c in preferred_cols if c in df.columns]
    df = df[existing_cols].copy()

    df["movie_key"] = pd.to_numeric(df["movie_key"], errors="coerce").astype("Int64")
    df["month_key"] = pd.to_numeric(df["month_key"], errors="coerce").astype("Int64")
    df["rating_count"] = pd.to_numeric(df["rating_count"], errors="coerce").fillna(0).astype(int)
    df["tag_count"] = pd.to_numeric(df["tag_count"], errors="coerce").fillna(0).astype(int)
    df["avg_rating"] = pd.to_numeric(df["avg_rating"], errors="coerce")

    df = df.sort_values(["month_key", "movie_key"]).reset_index(drop=True)
    return df


def validate_model(dim_movie: pd.DataFrame, dim_date: pd.DataFrame, fact: pd.DataFrame) -> None:
    duplicate_fact_rows = fact.duplicated(subset=["movie_key", "month_key"]).sum()
    null_movie_keys = fact["movie_key"].isna().sum()
    null_month_keys = fact["month_key"].isna().sum()

    valid_movie_keys = set(dim_movie["movie_key"].dropna().tolist())
    valid_month_keys = set(dim_date["month_key"].dropna().tolist())

    orphan_movie_keys = (~fact["movie_key"].isin(valid_movie_keys)).sum()
    orphan_month_keys = (~fact["month_key"].isin(valid_month_keys)).sum()

    print("\nFinal validation")
    print("-" * 30)
    print(f"Duplicate fact grain rows: {duplicate_fact_rows}")
    print(f"Null movie_key in fact: {null_movie_keys}")
    print(f"Null month_key in fact: {null_month_keys}")
    print(f"Fact rows with missing movie dimension key: {orphan_movie_keys}")
    print(f"Fact rows with missing date dimension key: {orphan_month_keys}")
    print(f"Final dim_movie rows: {len(dim_movie):,}")
    print(f"Final dim_date rows: {len(dim_date):,}")
    print(f"Final fact rows: {len(fact):,}")


def main() -> None:
    ensure_directories(FINAL_DIR)

    dim_date, dim_movie, fact_movie_month = load_exports()

    final_dim_movie = standardize_dim_movie(dim_movie)
    final_dim_date = standardize_dim_date(dim_date)
    final_fact = standardize_fact_movie_month(fact_movie_month)


    final_dim_movie.to_csv(FINAL_DIM_MOVIE_PATH, index=False)
    final_dim_date.to_csv(FINAL_DIM_DATE_PATH, index=False)
    final_fact.to_csv(FINAL_FACT_MOVIE_MONTH_PATH, index=False)

    validate_model(final_dim_movie, final_dim_date, final_fact)

    print("\nPhase 5 completed successfully.")
    print(f"Final dim_movie written to: {FINAL_DIM_MOVIE_PATH}")
    print(f"Final dim_date written to: {FINAL_DIM_DATE_PATH}")
    print(f"Final fact written to: {FINAL_FACT_MOVIE_MONTH_PATH}")


if __name__ == "__main__":
    main()