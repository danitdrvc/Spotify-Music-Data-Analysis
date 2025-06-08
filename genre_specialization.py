from pyspark.sql.functions import stddev, countDistinct, col, count

def genre_specialization_analysis(df):
    df = df.filter(col("popularity").isNotNull()).filter(col("popularity") > 0)
    
    artists_with_more_than_one_song = df.groupBy("artists") \
        .agg(count("*").alias("num_songs")) \
        .filter(col("num_songs") > 1)
        
    filtered_df = df.join(artists_with_more_than_one_song, "artists")
    
    # Calculate standard deviation of popularity per artist
    artist_stddev = filtered_df.groupBy("artists") \
                    .agg(stddev("popularity").alias("popularity_stddev")) \
                    .orderBy("popularity_stddev")
                    
    artist_stddev.filter(col("popularity_stddev").isNotNull()).show()

    # Identify artists with the lowest standard deviation (most consistent popularity)
    # Select the top 10 artists with the lowest standard deviation
    top_consistent_artists = artist_stddev.limit(10)

    # Analyze genre diversity for these artists
    # Count the number of unique genres for each artist
    artist_genre_diversity = df.groupBy("artists") \
                            .agg(countDistinct("track_genre").alias("genre_diversity"))

    # Join with the top consistent artists to get their genre diversity
    consistent_artists_genre_diversity = top_consistent_artists.join(artist_genre_diversity, "artists") \
        .select(
            "artists",
            "popularity_stddev",
            "genre_diversity"
        )

    # Show the results
    print("Top 10 artists with most consistent popularity and their genre diversity:")
    consistent_artists_genre_diversity.show(truncate=False)