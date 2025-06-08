from pyspark.sql.types import *
from pyspark.sql.functions import col, count, mean, stddev, min, max, approx_count_distinct, when
import json

def data_analysis(df):
    # Data Validation
    print("Schema Validation:")
    df.printSchema()

    print(f"Total number of rows: {df.count()}")

    print("\nSample Data Preview:")
    df.show(5, truncate=False)

    print("\nNull Values Count per Column:")
    null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])
    null_counts.show(truncate=False)

    # Perform Distribution Analysis
    analysis_results = []

    for field in df.schema.fields:
        column_name = field.name
        data_type = field.dataType
        stats = {}

        print(f"Analyzing column: {column_name}")

        if isinstance(data_type, (IntegerType, LongType, DoubleType)):
            # Numeric Columns: Compute statistical metrics
            numeric_agg = df.agg(
                count(column_name).alias("count"),
                mean(column_name).alias("mean"),
                stddev(column_name).alias("stddev"),
                min(column_name).alias("min"),
                max(column_name).alias("max")
            ).first()

            stats = {
                "count": numeric_agg["count"],
                "mean": float(numeric_agg["mean"]) if numeric_agg["mean"] is not None else None,
                "stddev": float(numeric_agg["stddev"]) if numeric_agg["stddev"] is not None else None,
                "min": float(numeric_agg["min"]) if isinstance(numeric_agg["min"], (int, float)) else None,
                "max": float(numeric_agg["max"]) if isinstance(numeric_agg["max"], (int, float)) else None
            }

        elif isinstance(data_type, BooleanType):
            # Boolean Columns: Count True, False, Nulls
            true_count = df.filter(col(column_name) == True).count()
            false_count = df.filter(col(column_name) == False).count()
            null_count = df.filter(col(column_name).isNull()).count()
            
            stats = {
                "true_count": true_count,
                "false_count": false_count,
                "null_count": null_count
            }

        elif isinstance(data_type, StringType):
            # String Columns: Distinct count and top values
            approx_distinct = df.select(approx_count_distinct(column_name)).first()[0]
            top_values = df.groupBy(column_name).count().orderBy(col("count").desc()).limit(3).collect()
            top_values_list = [{"value": row[column_name], "count": row["count"]} for row in top_values]
            
            stats = {
                "distinct_count": approx_distinct,
                "top_values": top_values_list
            }

        analysis_results.append({
            "column": column_name,
            "type": str(data_type),
            "statistics": stats
        })
    with open("spotify_distribution_analysis.json", "w") as json_file:
        json.dump(analysis_results, json_file, indent=4)