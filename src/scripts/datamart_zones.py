import sys

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
from pyspark.sql.window import Window 

PI = 3.14159265359
COEF_DEG_RAD = PI / 180

def main():
    path_events = sys.argv[1]
    path_geo_cities = sys.argv[2]
    path_to_write = sys.argv[3]

    conf = SparkConf().setAppName('datamart_users')
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)

    cities_df = cities(path_geo_cities, sql)
    events_df = events(path_events, sql)
    events_geo_df = events_geo(events_df, cities_df)
    mart_zones_df = mart_zones(events_geo_df)
    write = writer(mart_zones_df, path_to_write)  

    return write


def cities(path, session):
    cities_df = session.read \
        .option('header', True) \
        .option('delimiter', ';') \
        .csv(path) \
        .withColumn('lat_rad', F.regexp_replace('lat', ',', '.') * F.lit(COEF_DEG_RAD)) \
        .withColumn('lng_rad', F.regexp_replace('lng', ',', '.') * F.lit(COEF_DEG_RAD)) \
        .drop('lat', 'lng') \
        .persist()
    
    return cities_df

def events(path, session):
    events_df = session.read.parquet(path) \
        .select("event.message_id", "event.message_from", "event_type", "lat", "lon", "date") \
        .withColumn('msg_lat_rad', F.col('lat') * F.lit(COEF_DEG_RAD)) \
        .withColumn('msg_lng_rad', F.col('lon') * F.lit(COEF_DEG_RAD)) \
        .drop('lat', 'lon') \
        .persist() 
    
    return events_df

def events_geo(events_df, cities_df):
    events_geo_df = events_df \
        .crossJoin(cities_df) \
        .withColumn('distance',
            F.lit(2) * F.lit(6371) * F.asin(F.sqrt(
                F.pow(F.sin((F.col('msg_lat_rad') - F.col('lat_rad'))/F.lit(2)), 2)
                + F.cos(F.col("msg_lat_rad")) 
                * F.cos(F.col("lat_rad")) 
                * F.pow(F.sin((F.col('msg_lng_rad') - F.col('lng_rad'))/F.lit(2)),2)
            ))
        ) \
        .drop("msg_lat_rad","msg_lng_rad","lat_rad", "lng_rad")
        
    window = Window().partitionBy('message_id').orderBy(F.col('distance').asc())
    events_geo_df = events_geo_df \
        .withColumn("row_number", F.row_number().over(window)) \
        .filter(F.col('row_number')==1) \
        .drop('row_number', 'distance') \
        .withColumn('event_id', F.monotonically_increasing_id()) \
        .selectExpr("message_from as user_id","event_id", "event_type", "id as zone_id", "city", "date") \
        .persist()   
       
    return events_geo_df

def mart_zones(events_geo_df):
    window = Window().partitionBy('user_id').orderBy(F.col('date').asc())
    window_month = Window.partitionBy(['zone_id', F.trunc(F.col("date"), "month")])
    window_week = Window.partitionBy(['zone_id', F.trunc(F.col("date"), "week")])

    month_week_information_df = events_geo_df \
        .withColumn("row_number", F.row_number().over(window)) \
        .filter(F.col('row_number')==1) \
        .drop('row_number') \
        .withColumn("month",F.trunc(F.col("date"), "month")) \
        .withColumn("week",F.trunc(F.col("date"), "week")) \
        .withColumn("week_user", F.count('user_id').over(window_week)) \
        .withColumn("month_user", F.count('user_id').over(window_month)) \
        .selectExpr("month","week", "week_user", "month_user") \
        .distinct()

    mart_zones_df = events_geo_df \
        .withColumn("month",F.trunc(F.col("date"), "month")) \
        .withColumn("week",F.trunc(F.col("date"), "week")) \
        .withColumn("week_message",F.sum(F.when(events_geo_df.event_type == "message",1).otherwise(0)).over(window_week)) \
        .withColumn("week_reaction",F.sum(F.when(events_geo_df.event_type == "reaction",1).otherwise(0)).over(window_week)) \
        .withColumn("week_subscription",F.sum(F.when(events_geo_df.event_type == "subscription",1).otherwise(0)).over(window_week)) \
        .withColumn("month_message",F.sum(F.when(events_geo_df.event_type == "message",1).otherwise(0)).over(window_month)) \
        .withColumn("month_reaction",F.sum(F.when(events_geo_df.event_type == "reaction",1).otherwise(0)).over(window_month)) \
        .withColumn("month_subscription",F.sum(F.when(events_geo_df.event_type == "subscription",1).otherwise(0)).over(window_month)) \
        .join(month_week_information_df, ["month", "week"], "fullouter") \
        .select("month", 
                "week", 
                "zone_id", 
                "week_message", 
                "week_reaction", 
                "week_subscription", 
                "week_user", 
                "month_message", 
                "month_reaction", 
                "month_subscription", 
                "month_user") \
        .distinct()

    return mart_zones_df

def writer(df, output_path):
    return df \
        .write \
        .mode('overwrite') \
        .parquet(output_path)


if __name__ == "__main__":
    main()