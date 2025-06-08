from pyspark.sql.functions import col, corr

def correlation_analysis(df):
    # Convert the boolean 'explicit' column to numeric (1 for True, 0 for False)
    df = df.withColumn("explicit_numeric", col("explicit").cast("int"))

    # Group the data by genre and calculate correlation
    genre_correlation = df.groupBy("track_genre") \
                        .agg(corr("explicit_numeric", "popularity").alias("correlation")) \
                        .orderBy("track_genre")

    # Show the results
    print("Correlation between Explicit Content and Popularity by Genre:")
    genre_correlation.show(truncate=False)

    # Interpret the results
    # Identify genres with positive and negative correlation
    positive_correlation_genres = genre_correlation.filter(col("correlation") > 0)
    negative_correlation_genres = genre_correlation.filter(col("correlation") < 0)

    print("\nGenres with Positive Correlation (Explicit Content Helps Popularity):")
    positive_correlation_genres.show(truncate=False)

    print("\nGenres with Negative Correlation (Explicit Content Hurts Popularity):")
    negative_correlation_genres.show(truncate=False)