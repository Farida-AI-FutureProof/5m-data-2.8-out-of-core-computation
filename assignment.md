# Assignment

## Brief

Write the Python codes for the following questions.

## Instructions

Paste the answer as Python in the answer code section below each question.

### Question 1

Question: Join the `metadata_pl` and `ratings_pl` DataFrames in Polars, then calculate the average rating by `genre` and by `original_language` (separately).

Answer:

```python
import polars as pl

# Join metadata and ratings on a common key (e.g. movie_id)
# Adjust "movie_id" to the actual join key used in your notebook (e.g. "id" if different).
joined_pl = metadata_pl.join(
    ratings_pl,
    on="movie_id",
    how="inner"
)

# Average rating by genre
avg_rating_by_genre = (
    joined_pl
    .groupby("genre")
    .agg(
        pl.col("rating").mean().alias("avg_rating")
    )
    .sort("avg_rating", descending=True)
)

# Average rating by original_language
avg_rating_by_language = (
    joined_pl
    .groupby("original_language")
    .agg(
        pl.col("rating").mean().alias("avg_rating")
    )
    .sort("avg_rating", descending=True)
)

avg_rating_by_genre, avg_rating_by_language

```

### Question 2

Question: Calculate the average total amount for vendors with at least 5 trips from the NYC Taxi Trip Data.

Answer:

```python
import polars as pl

# nyc_taxi_pl is assumed to be already loaded, e.g.:
# nyc_taxi_pl = pl.read_parquet("data/nyc_taxi.parquet")  # or read_csv(...)

avg_total_amount_by_vendor = (
    nyc_taxi_pl
    .groupby("VendorID")
    .agg([
        pl.count().alias("trip_count"),
        pl.col("total_amount").mean().alias("avg_total_amount")
    ])
    .filter(pl.col("trip_count") >= 5)
)

avg_total_amount_by_vendor

```

## Submission

Used co-pilot and chat gpt to assist with code
Tested on Colab
