# BI Movie Project – Modeling and Pipeline Summary

## Project focus

The project analyzes monthly movie popularity, audience engagement, and audience reception. The final version does not include platform analysis or text-based sentiment analysis. Instead, it focuses on MovieLens rating and tag activity over time, enriched with IMDb and TMDb movie metadata.

The final fact table has the grain of one movie in one month. In plain terms, each fact row answers:

> “For movie X, in month Y, what measurable audience activity was observed?”

The main measures are:

- `rating_count`: number of ratings received by a movie in a month
- `avg_rating`: average rating received by a movie in a month
- `tag_count`: number of tags received by a movie in a month

The main dimensions are:

- Date
- Movie
- Genre

Optional movie attributes such as director, language, production country, production company, runtime, and release year are stored in the Movie dimension.

---

## Source data model

The source data comes from three public movie-related datasets.

### 1. MovieLens

MovieLens is the primary fact source because it contains timestamped user activity.

Main source entities:

- Movie
- Rating
- Tag
- External Link

Important attributes:

- Movie: `movieId`, `title`, `genres`
- Rating: `userId`, `movieId`, `rating`, `timestamp`
- Tag: `userId`, `movieId`, `tag`, `timestamp`
- Link: `movieId`, `imdbId`, `tmdbId`

Broad relationships:

- One movie can have many ratings.
- One movie can have many tags.
- One movie has external identifiers that connect it to IMDb and TMDb.

MovieLens ratings and tags are aggregated by movie and month to create the main fact table.

### 2. IMDb

IMDb is used as a metadata enrichment source.

Main source entities:

- IMDb Title
- IMDb Rating
- IMDb Crew
- IMDb Person / Director

Important attributes used:

- title type
- runtime
- release year
- IMDb average rating
- IMDb vote count
- director ID
- director name

IMDb data is connected to MovieLens through the IMDb ID from MovieLens `links.csv`. It enriches the Movie dimension but does not create a separate fact table.

### 3. TMDb

TMDb is used as an additional movie metadata enrichment source.

Main source entity:

- TMDb Movie Details

Important attributes used:

- original language
- popularity
- production company
- production country
- budget
- revenue
- release date
- vote average
- vote count

TMDb data is connected through the TMDb ID from MovieLens `links.csv`. Like IMDb, it enriches the Movie dimension and does not create a separate fact table.

---

## BI star schema model

The final BI model is a star schema with a bridge table for Genre.

### Fact table: `fact_movie_month`

Grain:

One row represents one movie in one month.

Foreign keys:

- `movie_key`
- `month_key`

Measures:

- `rating_count`
- `avg_rating`
- `tag_count`

This table is used to analyze movie popularity and audience reception over time.

---

### Dimension: `dim_date`

The Date dimension contains one row per month.

Important attributes:

- `month_key`
- `month_start`
- `year`
- `month_number`
- `month_name`
- `quarter`
- `year_month`
- `decade`

This dimension supports time-based analysis such as monthly trends, yearly comparisons, quarters, and decades.

---

### Dimension: `dim_movie`

The Movie dimension contains one row per movie.

Important attributes:

- `movie_key`
- `movie_id`
- `title`
- `release_year`
- `release_period`
- `runtime_minutes`
- `director_name`
- `tmdb_original_language`
- `tmdb_primary_country`
- `tmdb_primary_company`
- `imdb_avg_rating`
- `imdb_num_votes`
- `tmdb_popularity`
- `tmdb_vote_average`
- `tmdb_vote_count`

This dimension supports analysis by movie characteristics such as director, language, country, company, runtime, and release year.

---

### Dimension: `dim_genre`

The Genre dimension contains one row per genre.

Important attributes:

- `genre_key`
- `genre_name`

Genre is modeled as a separate dimension because it is one of the main analytical perspectives of the project.

---

### Bridge table: `bridge_movie_genre`

The bridge table connects movies to genres.

Important attributes:

- `movie_key`
- `genre_key`

This table is necessary because the relationship between movies and genres is many-to-many:

- One movie can belong to multiple genres.
- One genre can contain many movies.

Because of this, genre cannot be modeled properly as only one column in `dim_movie`. The bridge table preserves the full genre information from MovieLens.

When analyzing by genre, movies with multiple genres can appear in multiple genre groups. Therefore, genre-level totals represent genre participation, not mutually exclusive movie categories.

---

## Broad schema structure

<pre>
dim_date
   |
   | month_key
   |
fact_movie_month -------- dim_movie -------- bridge_movie_genre -------- dim_genre
        movie_key             movie_key              genre_key
</pre>

---

## Design choices

1. The fact grain is movie-month because MovieLens ratings and tags include timestamps.

2. The main measures are `rating_count`, `avg_rating`, and `tag_count` because they describe audience activity and audience reception.

3. Genre is modeled as a separate dimension because it is one of the core analytical perspectives.

4. A movie-genre bridge table is used because movies can belong to multiple genres.

5. IMDb and TMDb are used only as enrichment sources for the Movie dimension. They do not create additional facts.

6. Low-activity movies are filtered out to reduce noise and improve the quality of Tableau analysis.

7. Platform and sentiment analysis are excluded because the implemented pipeline does not include platform-specific monthly review facts or text sentiment scoring.

8. The final export is kept cleaner by removing unnecessary raw genre columns and using the separate Genre dimension instead.

---

## Final pipeline results

The final pipeline successfully produced a clean Tableau-ready BI dataset.

Final output sizes:

- `fact_movie_month`: 51,512 rows
- `dim_movie`: 1,297 rows
- `dim_date`: 264 rows
- `dim_genre`: 19 rows
- `bridge_movie_genre`: 3,653 rows

IMDb enrichment results:

- Rows in `dim_movie` before filtering: 9,742
- Matched IMDb basics: 9,074 movies, or 93.1%
- Matched IMDb ratings: 9,719 movies, or 99.8%
- Matched directors: 9,717 movies, or 99.7%
- Non-movie IMDb matches retained: 0

Filtering results:

- Original movies: 9,742
- Filtered movies: 1,297
- Movies removed: 8,445
- Original fact rows: 83,289
- Filtered fact rows: 51,512
- Fact rows removed: 31,777

TMDb enrichment results:

- Rows in `dim_movie` after filtering: 1,297
- Matched TMDb details: 1,295 movies, or 99.8%
- Rows with language: 1,295 movies, or 99.8%
- Rows with company: 1,295 movies, or 99.8%
- TMDb not_found rows: 2 movies, or 0.2%

Final validation results:

- Duplicate fact grain rows: 0
- Duplicate bridge rows: 0
- Bridge rows with missing movie dimension key: 0
- Bridge rows with missing genre dimension key: 0
- Null `movie_key` in fact: 0
- Null `month_key` in fact: 0
- Fact rows with missing movie dimension key: 0
- Fact rows with missing date dimension key: 0
- Negative `rating_count` rows: 0
- Negative `tag_count` rows: 0
- `avg_rating` rows below 0.5: 0
- `avg_rating` rows above 5.0: 0
- Movies with missing title: 0
- Movies without genre: 0

These results show that the final dataset is structurally clean and suitable for Tableau analysis.

---

## Final Tableau-ready files

The final warehouse consists of these files:

- `data/final/fact_movie_month.csv`
- `data/final/dim_date.csv`
- `data/final/dim_movie.csv`
- `data/final/dim_genre.csv`
- `data/final/bridge_movie_genre.csv`

---

## Refactored analytical questions

### Q1. Which movies generate the highest audience engagement over time?

Measures:

- `rating_count`
- `tag_count`

Dimensions:

- Movie
- Date

Benefit:

This helps identify movies with strong long-term interest compared to movies that only receive short-term attention spikes. It can support catalog prioritization and promotion decisions.

---

### Q2. Which genres sustain audience attention over multiple months?

Measures:

- `rating_count`
- `tag_count`
- active months

Dimensions:

- Genre
- Date

Benefit:

This shows which genres maintain durable audience interest over time and which genres are more short-lived.

---

### Q3. Are the most popular movies also the most positively received?

Measures:

- `rating_count`
- `avg_rating`

Dimensions:

- Movie
- Genre

Benefit:

This helps distinguish between movies that attract a lot of attention and movies that are actually rated highly by audiences. A movie may be popular but not highly rated, or highly rated but only watched by a smaller audience.

---

### Q4. How does audience reception differ across genres and release periods?

Measures:

- `avg_rating`

Dimensions:

- Genre
- Release Year / Release Period
- Date

Benefit:

This supports analysis of whether certain genres or movie eras tend to receive better audience ratings.

---

### Q5. Which movie attributes are associated with sustained popularity?

Measures:

- `rating_count`
- `tag_count`
- active months
- `avg_rating`

Dimensions:

- Genre
- Director
- Language
- Production Country
- Release Year / Release Period

Benefit:

This helps identify movie characteristics linked to long-term engagement and positive audience reception.