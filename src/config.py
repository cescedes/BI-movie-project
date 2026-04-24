from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
RAW_MOVIELENS_DIR = RAW_DIR / "movielens"
RAW_IMDB_DIR = RAW_DIR / "imdb"
RAW_TMDB_DIR = RAW_DIR / "tmdb"

TMDB_DETAILS_JSON_DIR = RAW_TMDB_DIR / "details"

STAGING_DIR = DATA_DIR / "staging"
EXPORT_DIR = DATA_DIR / "exports"

RATINGS_PATH = RAW_MOVIELENS_DIR / "ratings.csv"
TAGS_PATH = RAW_MOVIELENS_DIR / "tags.csv"
MOVIES_PATH = RAW_MOVIELENS_DIR / "movies.csv"
LINKS_PATH = RAW_MOVIELENS_DIR / "links.csv"

TITLE_BASICS_PATH = RAW_IMDB_DIR / "title.basics.tsv.gz"
TITLE_RATINGS_PATH = RAW_IMDB_DIR / "title.ratings.tsv.gz"
TITLE_CREW_PATH = RAW_IMDB_DIR / "title.crew.tsv.gz"
NAME_BASICS_PATH = RAW_IMDB_DIR / "name.basics.tsv.gz"

STAGING_MONTHLY_PATH = STAGING_DIR / "movielens_monthly.csv"
STAGING_TMDB_MOVIES_PATH = STAGING_DIR / "tmdb_movies.csv"

DIM_DATE_PATH = EXPORT_DIR / "dim_date.csv"
DIM_MOVIE_PATH = EXPORT_DIR / "dim_movie.csv"
FACT_MOVIE_MONTH_PATH = EXPORT_DIR / "fact_movie_month.csv"

TMDB_BASE_URL = "https://api.themoviedb.org/3"

FINAL_DIR = DATA_DIR / "final"

FINAL_DIM_DATE_PATH = FINAL_DIR / "dim_date.csv"
FINAL_DIM_MOVIE_PATH = FINAL_DIR / "dim_movie.csv"
FINAL_DIM_GENRE_PATH = FINAL_DIR / "dim_genre.csv"
FINAL_FACT_MOVIE_MONTH_PATH = FINAL_DIR / "fact_movie_month.csv"