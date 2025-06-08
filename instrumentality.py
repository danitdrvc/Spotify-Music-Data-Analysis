from pyspark.sql.functions import avg, col

def instrumentality_analysis(df):
    # Calculate average acousticness and instrumentalness per genre
    genre_avg_features = df.groupBy("track_genre") \
        .agg(
            avg("acousticness").alias("avg_acousticness"),
            avg("instrumentalness").alias("avg_instrumentalness"),
            avg("popularity").alias("avg_popularity")
        )

    # Filter genres with high acousticness and instrumentalness (>0.8)
    high_acoustic_instrumental_genres = genre_avg_features.filter(
        (col("avg_acousticness") > 0.7) & (col("avg_instrumentalness") > 0.7)
    )

    # Filter vocal-heavy genres
    vocal_heavy_genres = genre_avg_features.filter(
        (col("avg_acousticness") < 0.2) & (col("avg_instrumentalness") < 0.2)
    )

    # Calculate average popularity for both groups
    avg_popularity_high_acoustic_instrumental = high_acoustic_instrumental_genres.agg(
        avg("avg_popularity").alias("avg_popularity_high_acoustic_instrumental")
    ).first()[0]

    avg_popularity_vocal_heavy = vocal_heavy_genres.agg(
        avg("avg_popularity").alias("avg_popularity_vocal_heavy")
    ).first()[0]

    # Compare the results
    print("Average Popularity for High Acousticness and Instrumentalness Genres:")
    print(avg_popularity_high_acoustic_instrumental)

    print("Average Popularity for Vocal-Heavy Genres:")
    print(avg_popularity_vocal_heavy)