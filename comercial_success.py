from pyspark.sql.functions import col, avg

def comercial_success_analysis(df):
    # Filter songs with high danceability (danceability > 0.8)
    high_danceability_songs = df.filter(col("danceability") > 0.8)

    # Select the top 10 longest songs
    top_10_longest_songs = high_danceability_songs.orderBy(col("duration_ms").desc()).limit(10)

    # Calculate average popularity per genre
    genre_avg_popularity = df.groupBy("track_genre") \
                            .agg(avg("popularity").alias("avg_popularity"))

    # Compare popularity of top 10 longest songs with genre averages
    # Join the top 10 longest songs with genre averages
    comparison_df = top_10_longest_songs.join(genre_avg_popularity, "track_genre") \
        .select(
            "track_name",
            "track_genre",
            "duration_ms",
            "popularity",
            "avg_popularity",
        )

    # Show the results
    print("\nComparison with Genre Average Popularity:")
    comparison_df.show(truncate=False)