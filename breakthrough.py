from pyspark.sql.functions import col, count, avg, sum

def breakthrough_analysis(df):
    # Calculate average popularity per album (artists + album_name)
    album_avg = df.groupBy("artists", "album_name") \
                .agg(avg("popularity").alias("album_avg_popularity"))

    # Filter albums with average popularity <50
    low_avg_albums = album_avg.filter(col("album_avg_popularity") < 0.5)

    # Join to get all songs in low-average albums
    songs_in_low_avg_albums = df.join(low_avg_albums, ["artists", "album_name"])

    # Identify breakthrough songs (popularity >80)
    breakthrough_songs = songs_in_low_avg_albums.filter(col("popularity") > 0.8)

    # Compute averages of other songs in the album (excluding the breakthrough song)
    # First, calculate total sum and count for each album
    album_totals = songs_in_low_avg_albums.groupBy("artists", "album_name") \
        .agg(
            sum("energy").alias("total_energy"),
            sum("danceability").alias("total_danceability"),
            sum("valence").alias("total_valence"),
            count("energy").alias("total_songs")
        )

    # Join the totals back to the songs_in_low_avg_albums DataFrame
    songs_with_totals = songs_in_low_avg_albums.join(album_totals, ["artists", "album_name"])

    # Compute averages excluding the current song
    songs_with_avg_others = songs_with_totals.withColumn(
        "avg_energy_others",
        (col("total_energy") - col("energy")) / (col("total_songs") - 1)
    ).withColumn(
        "avg_danceability_others",
        (col("total_danceability") - col("danceability")) / (col("total_songs") - 1)
    ).withColumn(
        "avg_valence_others",
        (col("total_valence") - col("valence")) / (col("total_songs") - 1)
    )

    # Filter only breakthrough songs and calculate differences
    breakthrough_analysis = songs_with_avg_others.filter(col("popularity") > 0.8) \
        .select(
            "artists",
            "album_name",
            "track_name",
            "energy",
            "danceability",
            "valence",
            "avg_energy_others",
            "avg_danceability_others",
            "avg_valence_others",
            (col("energy") - col("avg_energy_others")).alias("energy_diff"),
            (col("danceability") - col("avg_danceability_others")).alias("danceability_diff"),
            (col("valence") - col("avg_valence_others")).alias("valence_diff")
        )

    # Show results
    breakthrough_analysis.show(truncate=False)