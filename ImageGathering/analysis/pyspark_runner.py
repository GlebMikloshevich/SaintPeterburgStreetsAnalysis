# %%
from pyspark.sql import types as T, functions as F, SparkSession
import pyspark
import json
import os

# %%
DEBUG = False

# %%


spark = SparkSession.builder \
                    .config("spark.jars", "/home/gleb/Downloads/postgresql-42.7.3.jar") \
                    .master("local") \
                    .config("spark.ui.port", "4040") \
                    .config("spark.driver.bindAddress", "0.0.0.0") \
                    .config("spark.driver.cores", "2") \
                    .config("spark.driver.memory", "8g") \
                    .config("spark.executor.memory", "4g") \
                    .config("spark.memory.fraction", "0.6") \
                    .config("spark.memory.storageFraction", "0.5") \
                    .appName("analysis") \
                    .getOrCreate()

df = spark.read.format("jdbc") \
          .option("url", "jdbc:postgresql://localhost:5432/street_analysis") \
          .option("driver", "org.postgresql.Driver") \
          .option("dbtable", "results") \
          .option("user", "spb") \
          .option("password", "spb") \
          .load()

cameras_df = spark.read.format("jdbc") \
          .option("url", "jdbc:postgresql://localhost:5432/street_analysis") \
          .option("driver", "org.postgresql.Driver") \
          .option("dbtable", "cameras") \
          .option("user", "spb") \
          .option("password", "spb") \
          .load()

# %%
if DEBUG:
    print(df.printSchema())
    print(cameras_df.printSchema())

# %%


def labels_deserialization(df: pyspark.sql.dataframe.DataFrame,
                           F: pyspark.sql.functions,
                           T: pyspark.sql.types):
    @F.udf(returnType=T.MapType(T.StringType(), T.IntegerType()))
    def udf(col):
        if col is None:
            return None
        return json.loads(col)
    df_json = df.withColumn("labels_des", udf(df["labels"]))
    return df_json


def explode_df(df: pyspark.sql.dataframe.DataFrame,
               F: pyspark.sql.functions):
    df = df.withColumn("person", F.col("labels_des")["person"]).withColumn(
        "car", F.col("labels_des")["car"]).withColumn(
        "motorcycle", F.col("labels_des")["motorcycle"]).withColumn(
        "bus", F.col("labels_des")["bus"]).withColumn(
        "truck", F.col("labels_des")["truck"]).withColumn(
        "boat", F.col("labels_des")["boat"])
    return df


def calculate_hourly_averages(df, group_by_camera=True):
    df = df.withColumn("hour", F.hour("created_at"))
    df = df.withColumn("day", F.day("created_at"))

    # Group by 'hour' and calculate average values
    avg_df = df.groupBy("hour", "camera_id").agg(
        F.avg("car").alias("avg_car"),
        F.avg("person").alias("avg_person"),
        F.avg("bus").alias("avg_bus"),
        F.avg("truck").alias("avg_truck")
    )
    return avg_df


def avg_objects(df: pyspark.sql.dataframe.DataFrame,
                F: pyspark.sql.functions):
    avg_df = df.groupBy("camera_id") \
            .agg(
                F.avg("person").alias("avg_person"),
                F.avg("car").alias("avg_car"),
                F.avg("motorcycle").alias("avg_motorcycle"),
                F.avg("bus").alias("avg_bus"),
                F.avg("truck").alias("avg_truck"))
    return avg_df


def sum_objects(df: pyspark.sql.dataframe.DataFrame,
                F: pyspark.sql.functions):
    sum_df = df.groupBy("camera_id") \
               .agg(
                    F.sum("person").alias("sum_person"),
                    F.sum("car").alias("sum_car"),
                    F.sum("motorcycle").alias("sum_motorcycle"),
                    F.sum("bus").alias("sum_bus"),
                    F.sum("truck").alias("sum_truck"))
    return sum_df


def join_df(df1: pyspark.sql.dataframe.DataFrame,
            df2: pyspark.sql.dataframe.DataFrame):
    return df1.join(df2, df1.camera_id == df2.id, "inner")


# %%
if not os.path.exists("./data"):
    os.makedirs("./data")
deserialized_df = labels_deserialization(df, F, T)
exploded_df = explode_df(deserialized_df, F)
sum_df = sum_objects(exploded_df, F)
hourly_average_df = calculate_hourly_averages(exploded_df)
hourly_average_cameras_df = join_df(hourly_average_df, cameras_df).drop("crop_list")
sum_cameras_df = join_df(sum_df, cameras_df).drop("crop_list")  # was joined

sum_cameras_df.drop("crop_list").toPandas().to_csv("./data/sum_df.csv")
hourly_average_cameras_df.toPandas().to_csv("./data/avg_df.csv")

# %%
