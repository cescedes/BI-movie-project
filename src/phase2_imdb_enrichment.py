""" 
from the project root, run with:
python src/phase2_imdb_enrichment.py

enriches: dim_movie.csv
"""

import pandas as pd

from config import (
    DIM_MOVIE_PATH,
    TITLE_BASICS_PATH,
    TITLE_RATINGS_PATH,
    TITLE_CREW_PATH,
    NAME_BASICS_PATH,
)
from utils import imdb_numeric_to_tconst, first_pipe_value, normalize_imdb_nulls


def load_dim_movie() -> pd.DataFrame:
    return pd.read_csv(DIM_MOVIE_PATH)


def load_imdb_sources() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    title_basics = pd.read_csv(
        TITLE_BASICS_PATH,
        sep="\t",
        usecols=[
            "tconst",
            "titleType",
            "primaryTitle",
            "originalTitle",
            "isAdult",
            "startYear",
            "runtimeMinutes",
            "genres",
        ],
        dtype="string",
        low_memory=False,
    )

    title_ratings = pd.read_csv(
        TITLE_RATINGS_PATH,
        sep="\t",
        usecols=["tconst", "averageRating", "numVotes"],
        dtype="string",
        low_memory=False,
    )

    title_crew = pd.read_csv(
        TITLE_CREW_PATH,
        sep="\t",
        usecols=["tconst", "directors"],
        dtype="string",
        low_memory=False,
    )

    name_basics = pd.read_csv(
        NAME_BASICS_PATH,
        sep="\t",
        usecols=["nconst", "primaryName"],
        dtype="string",
        low_memory=False,
    )

    title_basics = normalize_imdb_nulls(title_basics)
    title_ratings = normalize_imdb_nulls(title_ratings)
    title_crew = normalize_imdb_nulls(title_crew)
    name_basics = normalize_imdb_nulls(name_basics)

    return title_basics, title_ratings, title_crew, name_basics


def prepare_imdb_title_basics(title_basics: pd.DataFrame) -> pd.DataFrame:
    df = title_basics.copy()

    df = df[df["titleType"] == "movie"].copy()

    df["isAdult"] = pd.to_numeric(df["isAdult"], errors="coerce").astype("Int64")
    df["startYear"] = pd.to_numeric(df["startYear"], errors="coerce").astype("Int64")
    df["runtimeMinutes"] = pd.to_numeric(df["runtimeMinutes"], errors="coerce").astype("Int64")

    df = df.rename(
        columns={
            "tconst": "imdb_tconst",
            "titleType": "title_type",
            "isAdult": "is_adult",
            "startYear": "imdb_start_year",
            "runtimeMinutes": "runtime_minutes",
            "genres": "imdb_genres",
        }
    )

    return df[
        [
            "imdb_tconst",
            "title_type",
            "is_adult",
            "imdb_start_year",
            "runtime_minutes",
            "imdb_genres",
        ]
    ]


def prepare_imdb_title_ratings(title_ratings: pd.DataFrame) -> pd.DataFrame:
    df = title_ratings.copy()

    df["averageRating"] = pd.to_numeric(df["averageRating"], errors="coerce")
    df["numVotes"] = pd.to_numeric(df["numVotes"], errors="coerce").astype("Int64")

    df = df.rename(
        columns={
            "tconst": "imdb_tconst",
            "averageRating": "imdb_avg_rating",
            "numVotes": "imdb_num_votes",
        }
    )

    return df[["imdb_tconst", "imdb_avg_rating", "imdb_num_votes"]]


def prepare_imdb_directors(title_crew: pd.DataFrame, name_basics: pd.DataFrame) -> pd.DataFrame:
    crew = title_crew.copy()
    names = name_basics.copy()

    crew["director_ids"] = crew["directors"].apply(first_pipe_value)
    crew = crew.drop(columns=["directors"])

    names = names.rename(
        columns={
            "nconst": "director_ids",
            "primaryName": "director_name",
        }
    )

    directors = crew.merge(names, on="director_ids", how="left")
    directors = directors.rename(columns={"tconst": "imdb_tconst"})

    return directors[["imdb_tconst", "director_ids", "director_name"]]


def enrich_dim_movie(
    dim_movie: pd.DataFrame,
    imdb_basics: pd.DataFrame,
    imdb_ratings: pd.DataFrame,
    imdb_directors: pd.DataFrame,
) -> pd.DataFrame:
    df = dim_movie.copy()

    df["imdb_tconst"] = imdb_numeric_to_tconst(df["imdbId"])

    df = df.merge(imdb_basics, on="imdb_tconst", how="left")
    df = df.merge(imdb_ratings, on="imdb_tconst", how="left")
    df = df.merge(imdb_directors, on="imdb_tconst", how="left")

    # fallback: if MovieLens release_year is missing, fill from IMDb
    if "release_year" in df.columns:
        df["release_year"] = df["release_year"].fillna(df["imdb_start_year"])

    # fallback: if MovieLens genres are weak or missing, keep them but do not overwrite
    df["effective_genres"] = df["genres"].fillna(df["imdb_genres"])

    return df


def quality_report(df: pd.DataFrame) -> None:
    total = len(df)
    matched_basics = df["title_type"].notna().sum()
    matched_ratings = df["imdb_avg_rating"].notna().sum()
    matched_directors = df["director_name"].notna().sum()

    print("\nIMDb enrichment report")
    print("-" * 30)
    print(f"Rows in dim_movie: {total:,}")
    print(f"Matched IMDb basics:   {matched_basics:,} ({matched_basics / total:.1%})")
    print(f"Matched IMDb ratings:  {matched_ratings:,} ({matched_ratings / total:.1%})")
    print(f"Matched directors:     {matched_directors:,} ({matched_directors / total:.1%})")

    if "title_type" in df.columns:
        non_movie_matches = df["title_type"].dropna().ne("movie").sum()
        print(f"Non-movie IMDb matches retained: {non_movie_matches:,}")


def main() -> None:
    dim_movie = load_dim_movie()
    title_basics, title_ratings, title_crew, name_basics = load_imdb_sources()

    imdb_basics = prepare_imdb_title_basics(title_basics)
    imdb_ratings = prepare_imdb_title_ratings(title_ratings)
    imdb_directors = prepare_imdb_directors(title_crew, name_basics)

    enriched = enrich_dim_movie(dim_movie, imdb_basics, imdb_ratings, imdb_directors)

    enriched.to_csv(DIM_MOVIE_PATH, index=False)

    quality_report(enriched)
    print(f"\nUpdated file written to: {DIM_MOVIE_PATH}")


if __name__ == "__main__":
    main()