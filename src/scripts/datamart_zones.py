import sys

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
from pyspark.sql.window import Window 

def main():
    path_events = sys.argv[1]
    path_geo_cities = sys.argv[2]
    path_to_write = sys.argv[3]

    conf = SparkConf().setAppName('datamart_zones')
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
        .withColumn('lat_rad',  F.radians(F.regexp_replace('lat', ',', '.'))) \
        .withColumn('lng_rad', F.radians(F.regexp_replace('lng', ',', '.'))) \
        .drop('lat', 'lng')
    
    return cities_df

def events(path, session):
    events_df = session.read.parquet(f'{path}') \
        .selectExpr("date",
                    "event.user",
                    "event_type",
                    "event.message_id", 
                    "event.message_from",
                    "lat", 
                    "lon") \
        .where('lat is not null and lon is not null') \
        .withColumn('msg_lat_rad', F.radians(F.col('lat'))) \
        .withColumn('msg_lng_rad', F.radians(F.col('lon'))) \
        .drop('lat', 'lon') \
        .persist()
        
    return events_df

def events_geo(events_df, cities_df):
    window = Window().partitionBy('message_id').orderBy('distance')
    
    events_geo_df = events_df.crossJoin(cities_df) \
        .withColumn('distance',
            F.lit(2) * F.lit(6371) * F.asin(F.sqrt(
                F.pow(F.sin((F.col('msg_lat_rad') - F.col('lat_rad'))/F.lit(2)), 2)
                + F.cos(F.col("msg_lat_rad")) 
                * F.cos(F.col("lat_rad")) 
                * F.pow(F.sin((F.col('msg_lng_rad') - F.col('lng_rad'))/F.lit(2)),2)
            ))
        ) \
        .drop("msg_lat_rad","msg_lng_rad","lat_rad", "lng_rad") \
        .withColumn('row_number', F.row_number().over(window)) \
        .filter(F.col('row_number') == 1) \
        .withColumn('month', F.trunc(F.col('date'), 'month')) \
        .withColumn('week', F.trunc(F.col('date'), 'week')) \
        .selectExpr('month', 
                   'week', 
                   'date', 
                   'user', 
                   'event_type', 
                   'message_from', 
                   'id as zone_id') \
        .persist()
    
    return events_geo_df

def mart_zones(events_geo_df):
    window_rn = Window.partitionBy('message_from').orderBy(F.col('date').desc())
    window_week = Window().partitionBy('week', 'zone_id')
    window_month = Window().partitionBy('month', 'zone_id')
    
    registrations_df = events_geo_df \
        .where('message_from is not null') \
        .withColumn('rn', F.row_number().over(window_rn)) \
        .where('rn = 1') \
        .withColumn('week_user', F.count('message_from').over(window_week)) \
        .withColumn('month_user', F.count('message_from').over(window_month)) \
        .select('month', 'week', 'zone_id', 'week_user', 'month_user') \
        .distinct()
    
    mart_zones_df = events_geo_df \
        .withColumn('month', F.trunc(F.col('date'), 'month')) \
        .withColumn('week', F.trunc(F.col('date'), 'week')) \
        .withColumn('week_message', F.sum(F.when(F.col('event_type') == 'message', 1).otherwise(0)).over(window_week)) \
        .withColumn('week_reaction', F.sum(F.when(F.col('event_type') == 'reaction', 1).otherwise(0)).over(window_week)) \
        .withColumn('week_subscription', F.sum(F.when(F.col('event_type') == 'subscription', 1).otherwise(0)).over(window_week)) \
        .withColumn('month_message', F.sum(F.when(F.col('event_type') == 'message', 1).otherwise(0)).over(window_month)) \
        .withColumn('month_reaction', F.sum(F.when(F.col('event_type') == 'reaction', 1).otherwise(0)).over(window_month)) \
        .withColumn('month_subscription', F.sum(F.when(F.col('event_type') == 'subscription', 1).otherwise(0)).over(window_month)) \
        .join(registrations_df, ['month', 'week', 'zone_id'], 'left') \
        .selectExpr('month', 
                    'week', 
                    'zone_id', 
                    'week_message', 
                    'week_reaction', 
                    'week_subscription',
                    'week_user',
                    'month_message',
                    'month_reaction',
                    'month_subscription',
                    'month_user'
                   ) \
        .orderBy('month', 'week', 'zone_id') \
        .distinct()
   
    return mart_zones_df

def writer(df, output_path):
    return df \
        .coalesce(1) \
        .write \
        .mode('overwrite') \
        .parquet(output_path)


if __name__ == "__main__":
    main()