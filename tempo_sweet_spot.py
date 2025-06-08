from pyspark.sql.functions import col, avg, when


def tempo_sweet_spot_analysis(df):
    # Calculate average popularity per genre

    genre_avg_popularity = df.groupBy("track_genre") \
                            .agg(avg("popularity").alias("avg_popularity")) \
                            .orderBy(col("avg_popularity").desc())

    # Get the top 5 genres
    top_5_genres = genre_avg_popularity.limit(5)

    # Filter data for the top 5 genres
    top_5_genres_list = [row["track_genre"] for row in top_5_genres.collect()]
    filtered_df = df.filter(col("track_genre").isin(top_5_genres_list))

    # Categorize songs into tempo ranges
    tempo_ranges_df = filtered_df.withColumn(
        "tempo_range",
        when(col("tempo") < 100, "<100 BPM")
        .when((col("tempo") >= 100) & (col("tempo") <= 120), "100-120 BPM")
        .otherwise(">120 BPM")
    )

    # Calculate average popularity per genre and tempo range
    genre_tempo_avg_popularity = tempo_ranges_df.groupBy("track_genre", "tempo_range") \
                                                .agg(avg("popularity").alias("avg_popularity")) \
                                                .orderBy("track_genre", "tempo_range")

    # Show the results
    print("Top 5 Genres by Average Popularity:")
    top_5_genres.show(truncate=False)

    print("\nAverage Popularity per Genre and Tempo Range:")
    genre_tempo_avg_popularity.show(truncate=False)
