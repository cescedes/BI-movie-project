""" 
from the project root, run with:
python src/phase3_filter_and_rebuild.py

it rewrites the exported files with a cleaner subset:
- dim_movie.csv
- dim_date.csv
- fact_movie_month.csv

the exports are now the analysis-ready version.
"""

import pandas as pd

from config import (
    DIM_MOVIE_PATH,
    DIM_DATE_PATH,
    FACT_MOVIE_MONTH_PATH,
    STAGING_MOVIE_ACTIVITY_STATS_PATH,
    MIN_TOTAL_RATINGS,
    MIN_ACTIVE_MONTHS,
    MIN_RELEASE_YEAR,
    USE_RELEASE_YEAR_FILTER,
)


def load_current_exports() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    dim_movie = pd.read_csv(DIM_MOVIE_PATH)
    dim_date = pd.read_csv(DIM_DATE_PATH)
    fact_movie_month = pd.read_csv(FACT_MOVIE_MONTH_PATH)

    return dim_movie, dim_date, fact_movie_month


def build_movie_activity_stats(fact_movie_month: pd.DataFrame) -> pd.DataFrame:
    stats = (
        fact_movie_month.groupby("movie_key", as_index=False)
        .agg(
            total_ratings=("rating_count", "sum"),
            total_tags=("tag_count", "sum"),
            active_months=("month_key", "nunique"),
            avg_monthly_rating=("avg_rating", "mean"),
        )
    )

    return stats


def filter_movies(dim_movie: pd.DataFrame, movie_stats: pd.DataFrame) -> pd.DataFrame:
    df = dim_movie.merge(movie_stats, on="movie_key", how="left")

    df["total_ratings"] = df["total_ratings"].fillna(0).astype(int)
    df["total_tags"] = df["total_tags"].fillna(0).astype(int)
    df["active_months"] = df["active_months"].fillna(0).astype(int)

    mask = (
        (df["total_ratings"] >= MIN_TOTAL_RATINGS) &
        (df["active_months"] >= MIN_ACTIVE_MONTHS)
    )

    if "title_type" in df.columns:
        mask = mask & (df["title_type"].isna() | (df["title_type"] == "movie"))

    if USE_RELEASE_YEAR_FILTER and "release_year" in df.columns:
        mask = mask & (df["release_year"].fillna(0) >= MIN_RELEASE_YEAR)

    filtered = df.loc[mask].copy()

    return filtered


def filter_fact_table(
    fact_movie_month: pd.DataFrame,
    kept_movie_keys: pd.Series,
) -> pd.DataFrame:
    fact_filtered = fact_movie_month[
        fact_movie_month["movie_key"].isin(kept_movie_keys)
    ].copy()

    fact_filtered = fact_filtered.sort_values(["month_key", "movie_key"]).reset_index(drop=True)

    return fact_filtered


def filter_date_dimension(
    dim_date: pd.DataFrame,
    fact_filtered: pd.DataFrame,
) -> pd.DataFrame:
    used_month_keys = fact_filtered["month_key"].dropna().unique()

    dim_date_filtered = dim_date[
        dim_date["month_key"].isin(used_month_keys)
    ].copy()

    dim_date_filtered = dim_date_filtered.sort_values("month_key").reset_index(drop=True)

    return dim_date_filtered


def finalize_dim_movie(filtered_movies: pd.DataFrame) -> pd.DataFrame:
    keep_cols = [col for col in filtered_movies.columns if col not in {
        "total_ratings", "total_tags", "active_months", "avg_monthly_rating"
    }]

    final_dim_movie = filtered_movies[keep_cols].copy()
    final_dim_movie = final_dim_movie.sort_values("movie_key").reset_index(drop=True)

    return final_dim_movie


def validate_outputs(
    dim_movie: pd.DataFrame,
    dim_date: pd.DataFrame,
    fact_movie_month: pd.DataFrame,
) -> None:
    dupes = fact_movie_month.duplicated(subset=["movie_key", "month_key"]).sum()

    print("\nValidation")
    print("-" * 30)
    print(f"Duplicate fact grain rows: {dupes}")
    print(f"Null movie_key in fact: {fact_movie_month['movie_key'].isna().sum()}")
    print(f"Null month_key in fact: {fact_movie_month['month_key'].isna().sum()}")
    print(f"Movies in dim_movie: {len(dim_movie):,}")
    print(f"Months in dim_date: {len(dim_date):,}")
    print(f"Rows in fact_movie_month: {len(fact_movie_month):,}")


def summary_report(
    original_dim_movie: pd.DataFrame,
    original_fact: pd.DataFrame,
    filtered_dim_movie: pd.DataFrame,
    filtered_fact: pd.DataFrame,
) -> None:
    print("\nFiltering summary")
    print("-" * 30)
    print(f"Original movies: {len(original_dim_movie):,}")
    print(f"Filtered movies: {len(filtered_dim_movie):,}")
    print(f"Movies removed: {len(original_dim_movie) - len(filtered_dim_movie):,}")
    print()
    print(f"Original fact rows: {len(original_fact):,}")
    print(f"Filtered fact rows: {len(filtered_fact):,}")
    print(f"Fact rows removed: {len(original_fact) - len(filtered_fact):,}")


def main() -> None:
    dim_movie, dim_date, fact_movie_month = load_current_exports()

    original_dim_movie = dim_movie.copy()
    original_fact = fact_movie_month.copy()

    movie_stats = build_movie_activity_stats(fact_movie_month)
    movie_stats.to_csv(STAGING_MOVIE_ACTIVITY_STATS_PATH, index=False)

    filtered_movies = filter_movies(dim_movie, movie_stats)

    final_dim_movie = finalize_dim_movie(filtered_movies)
    final_fact = filter_fact_table(fact_movie_month, final_dim_movie["movie_key"])
    final_dim_date = filter_date_dimension(dim_date, final_fact)

    final_dim_movie.to_csv(DIM_MOVIE_PATH, index=False)
    final_dim_date.to_csv(DIM_DATE_PATH, index=False)
    final_fact.to_csv(FACT_MOVIE_MONTH_PATH, index=False)

    summary_report(original_dim_movie, original_fact, final_dim_movie, final_fact)
    validate_outputs(final_dim_movie, final_dim_date, final_fact)

    print(f"\nMovie activity stats written to: {STAGING_MOVIE_ACTIVITY_STATS_PATH}")
    print("\nPhase 3 completed successfully.")


if __name__ == "__main__":
    main()