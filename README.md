# BI Movie Project – Data Processing Pipeline

This project builds a small movie-oriented BI warehouse for analyzing **monthly movie popularity and audience sentiment/activity**.

## Project goal

The pipeline integrates public movie datasets and transforms them into a Tableau-ready star schema.

The final warehouse is centered on a monthly fact table:

**one row = one movie in one month**

with measures such as:
- `rating_count`
- `avg_rating`
- `tag_count`

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
Applies filtering rules to remove sparse or low-signal movies and rebuilds cleaner exports for analysis.

### Phase 4 – TMDb enrichment
Fetches and caches TMDb movie details, then enriches `dim_movie.csv` with:
- original language
- popularity
- budget / revenue
- primary production company
- primary production country

### Phase 5 – Final export packaging
Creates the final cleaned files for Tableau in the `data/final/` folder.

## Folder structure

- `data/raw/` – raw input files
- `data/staging/` – intermediate processed files
- `data/exports/` – working warehouse exports
- `data/final/` – final Tableau-ready files
- `src/` – Python pipeline scripts

## How to run

Run the phases in order:

```bash
python src/phase1_movielens.py
python src/phase2_imdb_enrichment.py
python src/phase3_filter_and_rebuild.py
python src/phase4_tmdb_enrichment.py
python src/phase5_finalize_exports.py


