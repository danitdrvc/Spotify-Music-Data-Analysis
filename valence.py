from pyspark.sql.functions import col, avg

def valence_analysis(df):
    # Group songs by explicitness and calculate average valence
    explicit_avg_valence = df.filter(col("explicit") == True) \
                            .agg(avg("valence").alias("avg_valence_explicit"))
    non_explicit_avg_valence = df.filter(col("explicit") == False) \
                                .agg(avg("valence").alias("avg_valence_non_explicit"))

    # Show overall average valence for explicit and non-explicit songs
    print("Overall Average Valence:")
    explicit_avg_valence.show()
    non_explicit_avg_valence.show()

    # Divide popularity into bins of size 0.1
    # Create a new column 'popularity_bin' to represent the bin
    df = df.withColumn("popularity_bin", (col("popularity") / 10).cast("int") * 10)

    # Calculate average valence per popularity bin for explicit and non-explicit songs
    explicit_valence_by_bin = df.filter(col("explicit") == True) \
                                .groupBy("popularity_bin") \
                                .agg(avg("valence").alias("avg_valence_explicit")) \
                                .orderBy("popularity_bin")

    non_explicit_valence_by_bin = df.filter(col("explicit") == False) \
                                    .groupBy("popularity_bin") \
                                    .agg(avg("valence").alias("avg_valence_non_explicit")) \
                                    .orderBy("popularity_bin")

    # Join the results for comparison
    valence_comparison = explicit_valence_by_bin.join(non_explicit_valence_by_bin, "popularity_bin") \
        .select(
            "popularity_bin",
            "avg_valence_explicit",
            "avg_valence_non_explicit",
            (col("avg_valence_explicit") - col("avg_valence_non_explicit")).alias("valence_diff")
        )

    # Show the results
    print("\nAverage Valence by Popularity Bin:")
    valence_comparison.orderBy("popularity_bin").show(truncate=False)