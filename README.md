# BI Movie Project – Data Processing Pipeline

This project builds a small movie-oriented BI warehouse for analyzing **monthly movie popularity, audience engagement, and audience reception**.

## Project goal

The pipeline integrates public movie datasets and transforms them into a Tableau-ready warehouse with a monthly fact table and supporting dimensions. The final warehouse uses a fact table at the grain of one movie in one month. The core dimensions are Date, Movie, and Genre. Since movies can belong to multiple genres, Genre is modeled as a separate dimension connected to Movie through a bridge table. Additional analytical attributes such as Director, Language, Production Country, and Release Year are stored in the Movie dimension.

## Data sources

The pipeline currently uses:

- **MovieLens** – main behavioral source for ratings, tags, movies, and external links

[F. Maxwell Harper and Joseph A. Konstan. 2015. The MovieLens Datasets: History and Context. ACM Transactions on Interactive Intelligent Systems (TiiS) 5, 4: 19:1–19:19. <https://doi.org/10.1145/2827872>]
- **IMDb** – enrichment for runtime, release year, ratings, votes, and directors
- **TMDb** – enrichment for language, popularity, company, country, budget, and revenue

[This product uses the TMDB API but is not endorsed or certified by TMDB.]

## Pipeline phases

### Phase 1 – MovieLens transformation
Builds the first monthly fact table and base dimensions from:
- `ratings.csv`
- `tags.csv`
- `movies.csv`
- `links.csv`

Outputs:
- `dim_date.csv`
- `dim_movie.csv`
- `dim_genre.csv`
- `bridge_movie_genre.csv`
- `fact_movie_month.csv`

### Phase 2 – IMDb enrichment
Enriches `dim_movie.csv` with:
- IMDb title type
- runtime
- release year
- IMDb average rating
- IMDb vote count
- director name

### Phase 3 – Filtering and rebuild
Applies filtering rules to remove sparse or low-signal movies and rebuilds cleaner exports for analysis. The genre bridge table is also filtered so it only contains movies that remain in the final analysis set.

### Phase 4 – TMDb enrichment
Fetches and caches TMDb movie details, then enriches `dim_movie.csv` with:
- original language
- popularity
- budget / revenue
- primary production company
- primary production country

### Phase 5 – Final export packaging
Creates the final cleaned files for Tableau in the `data/final/` folder, including the fact table, core dimensions, and the movie-genre bridge table.

## Folder structure

- `data/raw/` – raw input files
- `data/staging/` – intermediate processed files
- `data/exports/` – working warehouse exports
- `data/final/` – final Tableau-ready files
- `src/` – Python pipeline scripts

## How to run

Run order for full dataset build:
```bash 
python src/phase1_movielens.py
python src/phase2_imdb_enrichment.py
python src/phase3_filter_and_rebuild.py
python src/phase4_tmdb_enrichment.py
python src/phase5_finalize_exports.py
```

Or run everything with:
```bash 
python src/run_pipeline.py
```

## Final output

The final warehouse files are:

- `data/final/fact_movie_month.csv`
- `data/final/dim_date.csv`
- `data/final/dim_movie.csv`
- `data/final/dim_genre.csv`
- `data/final/bridge_movie_genre.csv`

## Final schema

The fact table has one row per movie per month.

`fact_movie_month.csv`
- `movie_key`
- `month_key`
- `rating_count`
- `avg_rating`
- `tag_count`

`dim_date.csv`
- Date and calendar attributes for each month.

`dim_movie.csv`
- Movie-level metadata such as title, release year, runtime, director, language, production company, and production country.

`dim_genre.csv`
- One row per genre.

`bridge_movie_genre.csv`
- Connects movies to genres. This bridge is required because one movie can belong to multiple genres.