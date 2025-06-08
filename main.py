from pyspark.sql import SparkSession
from pyspark.sql.types import *
from data_analysis import data_analysis
from collaboration import collaboration_analysis
from breakthrough import breakthrough_analysis
from tempo_sweet_spot import tempo_sweet_spot_analysis
from correlation import correlation_analysis
from comercial_success import comercial_success_analysis
from valence import valence_analysis
from genre_specialization import genre_specialization_analysis 
from instrumentality import instrumentality_analysis

if __name__ == "__main__":
    # Initialize Spark Session
    spark = SparkSession.builder.appName("SpotifyDataAnalysis").getOrCreate()

    # Define the schema based on the provided structure
    schema = StructType([
        StructField("", IntegerType(), True),
        StructField("track_id", StringType(), True),
        StructField("artists", StringType(), True),
        StructField("album_name", StringType(), True),
        StructField("track_name", StringType(), True),
        StructField("popularity", IntegerType(), True),
        StructField("duration_ms", LongType(), True),
        StructField("explicit", BooleanType(), True),
        StructField("danceability", DoubleType(), True),
        StructField("energy", DoubleType(), True),
        StructField("key", IntegerType(), True),
        StructField("loudness", DoubleType(), True),
        StructField("mode", IntegerType(), True),
        StructField("speechiness", DoubleType(), True),
        StructField("acousticness", DoubleType(), True),
        StructField("instrumentalness", DoubleType(), True),
        StructField("liveness", DoubleType(), True),
        StructField("valence", DoubleType(), True),
        StructField("tempo", DoubleType(), True),
        StructField("time_signature", IntegerType(), True),
        StructField("track_genre", StringType(), True)
    ])

    # Load the data
    df = spark.read.csv("dataset.csv", schema=schema, header=True)
    
    # 1) ----------------------------------------------
    # data_analysis(df)
    
    # 2) ----------------------------------------------
    # collaboration_analysis(df)
    
    # 3) ----------------------------------------------
    # breakthrough_analysis(df)
    
    # 4) ----------------------------------------------
    # tempo_sweet_spot_analysis(df)
    
    # 5) ----------------------------------------------
    # correlation_analysis(df)
    
    # 6) ----------------------------------------------
    # comercial_success_analysis(df)
    
    # 7) ----------------------------------------------
    # valence_analysis(df)
    
    # 8) ----------------------------------------------
    # genre_specialization_analysis(df)
    
    # 9) ----------------------------------------------
    # instrumentality_analysis(df)
    
    # Stop the Spark session
    spark.stop()

