""" 
from the project root, run with:
python src/phase1_movielens.py

should export:

data/staging/movielens_monthly.csv
data/exports/dim_movie.csv
data/exports/dim_date.csv
data/exports/fact_movie_month.csv
"""

import pandas as pd

from config import (
    RATINGS_PATH,
    TAGS_PATH,
    MOVIES_PATH,
    LINKS_PATH,
    STAGING_DIR,
    EXPORT_DIR,
    STAGING_MONTHLY_PATH,
    DIM_DATE_PATH,
    DIM_MOVIE_PATH,
    FACT_MOVIE_MONTH_PATH,
)
from utils import ensure_directories, unix_to_month_start, parse_primary_genre, safe_int_key


def load_movielens_data() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    ratings = pd.read_csv(
        RATINGS_PATH,
        usecols=["userId", "movieId", "rating", "timestamp"],
        dtype={"userId": "int32", "movieId": "int32", "rating": "float32", "timestamp": "int64"},
    )

    tags = pd.read_csv(
        TAGS_PATH,
        usecols=["userId", "movieId", "tag", "timestamp"],
        dtype={"userId": "int32", "movieId": "int32", "tag": "string", "timestamp": "int64"},
    )

    movies = pd.read_csv(
        MOVIES_PATH,
        usecols=["movieId", "title", "genres"],
        dtype={"movieId": "int32", "title": "string", "genres": "string"},
    )

    links = pd.read_csv(
        LINKS_PATH,
        usecols=["movieId", "imdbId", "tmdbId"],
        dtype={"movieId": "int32"},
    )

    return ratings, tags, movies, links


def prepare_movies_dimension(movies: pd.DataFrame, links: pd.DataFrame) -> pd.DataFrame:
    movies = movies.copy()
    links = links.copy()

    links["imdbId"] = safe_int_key(links["imdbId"])
    links["tmdbId"] = safe_int_key(links["tmdbId"])

    dim_movie = movies.merge(links, on="movieId", how="left")

    dim_movie["primary_genre"] = dim_movie["genres"].apply(parse_primary_genre)

    # Basic release year extraction from title, e.g. "Toy Story (1995)"
    dim_movie["release_year"] = (
        dim_movie["title"]
        .str.extract(r"\((\d{4})\)$", expand=False)
        .pipe(pd.to_numeric, errors="coerce")
        .astype("Int64")
    )

    # Create both keys explicitly
    dim_movie["movie_key"] = dim_movie["movieId"]
    dim_movie["movie_id"] = dim_movie["movieId"]

    dim_movie = dim_movie[
        [
            "movie_key",
            "movie_id",
            "title",
            "genres",
            "primary_genre",
            "release_year",
            "imdbId",
            "tmdbId",
        ]
    ].drop_duplicates()

    return dim_movie


def aggregate_ratings_monthly(ratings: pd.DataFrame) -> pd.DataFrame:
    ratings = ratings.copy()
    ratings["month_start"] = unix_to_month_start(ratings["timestamp"])

    ratings_monthly = (
        ratings.groupby(["movieId", "month_start"], as_index=False)
        .agg(
            rating_count=("rating", "size"),
            avg_rating=("rating", "mean"),
        )
    )

    ratings_monthly["avg_rating"] = ratings_monthly["avg_rating"].round(4)
    return ratings_monthly


def aggregate_tags_monthly(tags: pd.DataFrame) -> pd.DataFrame:
    tags = tags.copy()
    tags["month_start"] = unix_to_month_start(tags["timestamp"])

    tags_monthly = (
        tags.groupby(["movieId", "month_start"], as_index=False)
        .agg(
            tag_count=("tag", "size"),
        )
    )

    return tags_monthly


def build_monthly_staging(
    ratings_monthly: pd.DataFrame,
    tags_monthly: pd.DataFrame,
    dim_movie: pd.DataFrame,
) -> pd.DataFrame:
    monthly = ratings_monthly.merge(
        tags_monthly,
        on=["movieId", "month_start"],
        how="outer",
    )

    monthly["rating_count"] = monthly["rating_count"].fillna(0).astype(int)
    monthly["tag_count"] = monthly["tag_count"].fillna(0).astype(int)

    monthly = monthly.merge(
        dim_movie[
            [
                "movie_key",
                "title",
                "genres",
                "primary_genre",
                "release_year",
                "imdbId",
                "tmdbId",
            ]
        ].rename(columns={"movie_key": "movieId"}),
        on="movieId",
        how="left",
    )

    monthly = monthly.rename(columns={"movieId": "movie_key"})

    monthly = monthly[
        [
            "movie_key",
            "month_start",
            "rating_count",
            "avg_rating",
            "tag_count",
            "title",
            "genres",
            "primary_genre",
            "release_year",
            "imdbId",
            "tmdbId",
        ]
    ].sort_values(["month_start", "movie_key"])

    return monthly


def build_dim_date(monthly: pd.DataFrame) -> pd.DataFrame:
    dim_date = monthly[["month_start"]].drop_duplicates().sort_values("month_start").copy()

    dim_date["month_key"] = dim_date["month_start"].dt.strftime("%Y%m").astype(int)
    dim_date["year"] = dim_date["month_start"].dt.year
    dim_date["month_number"] = dim_date["month_start"].dt.month
    dim_date["month_name"] = dim_date["month_start"].dt.month_name()
    dim_date["quarter"] = "Q" + dim_date["month_start"].dt.quarter.astype(str)
    dim_date["year_month"] = dim_date["month_start"].dt.strftime("%Y-%m")
    dim_date["decade"] = (dim_date["year"] // 10) * 10

    dim_date = dim_date[
        [
            "month_key",
            "month_start",
            "year",
            "month_number",
            "month_name",
            "quarter",
            "year_month",
            "decade",
        ]
    ]

    return dim_date


def build_fact_movie_month(monthly: pd.DataFrame, dim_date: pd.DataFrame) -> pd.DataFrame:
    fact = monthly.merge(
        dim_date[["month_key", "month_start"]],
        on="month_start",
        how="left",
    )

    fact = fact[
        [
            "movie_key",
            "month_key",
            "rating_count",
            "avg_rating",
            "tag_count",
        ]
    ].copy()

    fact = fact.sort_values(["month_key", "movie_key"]).reset_index(drop=True)
    return fact


def main() -> None:
    ensure_directories(STAGING_DIR, EXPORT_DIR)

    ratings, tags, movies, links = load_movielens_data()

    dim_movie = prepare_movies_dimension(movies, links)
    ratings_monthly = aggregate_ratings_monthly(ratings)
    tags_monthly = aggregate_tags_monthly(tags)

    monthly = build_monthly_staging(ratings_monthly, tags_monthly, dim_movie)
    dim_date = build_dim_date(monthly)
    fact_movie_month = build_fact_movie_month(monthly, dim_date)

    monthly.to_csv(STAGING_MONTHLY_PATH, index=False)
    dim_movie.to_csv(DIM_MOVIE_PATH, index=False)
    dim_date.to_csv(DIM_DATE_PATH, index=False)
    fact_movie_month.to_csv(FACT_MOVIE_MONTH_PATH, index=False)

    print("Phase 1 completed successfully.")
    print(f"Rows in dim_movie: {len(dim_movie):,}")
    print(f"Rows in dim_date: {len(dim_date):,}")
    print(f"Rows in fact_movie_month: {len(fact_movie_month):,}")


if __name__ == "__main__":
    main()