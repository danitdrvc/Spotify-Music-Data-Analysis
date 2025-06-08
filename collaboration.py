from pyspark.sql.functions import col, count, min, split, concat_ws, array_sort, size, avg, explode

def collaboration_analysis(df):
    # Process collaborations
    # Split artists into an array, sort, and create a collaboration key
    collab_df = df.withColumn("artists_array", split(col("artists"), ";")) \
                .withColumn("collab_key", concat_ws(",", array_sort("artists_array"))) \
                .filter(size("artists_array") >= 2)

    # Get top 10 collaborations by count
    top_collabs_count = collab_df.groupBy("collab_key") \
                                .agg(count("*").alias("collab_count")) \
                                .orderBy(col("collab_count").desc()) \
                                .limit(10)

    # Calculate average popularity for each collaboration
    collab_avg_popularity = collab_df.groupBy("collab_key") \
                                    .agg(avg("popularity").alias("avg_collab_popularity"))

    # Join count and average popularity
    top_collabs = top_collabs_count.join(collab_avg_popularity, "collab_key")

    # Calculate solo average popularity for each artist
    # Process solo tracks (artists with only one member)
    solo_df = df.withColumn("artists_array", split(col("artists"), ";")) \
                .filter(size("artists_array") == 1) \
                .withColumn("artist", explode("artists_array"))

    solo_avg_popularity = solo_df.groupBy("artist") \
                                .agg(avg("popularity").alias("solo_avg_popularity"))

    # For each top collaboration, find the minimum solo average among its artists
    # Split collab_key into individual artists
    collab_artists = top_collabs.withColumn("artist", explode(split(col("collab_key"), ",")))

    # Join with solo averages (left join in case an artist has no solo tracks)
    collab_with_solo_avg = collab_artists.join(solo_avg_popularity, "artist", "left")

    # Find the minimum solo average per collaboration
    min_solo_avg = collab_with_solo_avg.groupBy("collab_key") \
                                    .agg(min("solo_avg_popularity").alias("min_solo_avg"))

    # Compute the difference between collaboration average and minimum solo average
    final_result = top_collabs.join(min_solo_avg, "collab_key") \
                            .withColumn("popularity_difference", 
                                        col("avg_collab_popularity") - col("min_solo_avg")) \
                            .select(
                                col("collab_key"),
                                col("collab_count"),
                                col("avg_collab_popularity"),
                                col("min_solo_avg"),
                                col("popularity_difference")
                            ).orderBy(col("collab_count").desc())

    # Show the result
    final_result.show(10, truncate=False)