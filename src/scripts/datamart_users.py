import sys

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
from pyspark.sql.window import Window 
from pyspark.sql.types import DateType

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
    actual_city_df = actual_city(events_geo_df)
    home_city_df = home_city(events_geo_df)
    travel_city_df = travel_city(events_geo_df)
    time_info_df = time_info(events_geo_df)
    mart_users_df = mart_users(events_geo_df, actual_city_df, home_city_df, travel_city_df, time_info_df)
    write = writer(mart_users_df, path_to_write)

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
    events_df = session.read.parquet(f'{path}/event_type=message') \
        .selectExpr("event.message_id", 
                    "event.message_from as user_id", 
                    "date",
                    "event.datetime as datetime",
                    "lat", 
                    "lon") \
        .where("user_id is not null") \
        .withColumn('msg_lat_rad', F.radians(F.col('lat'))) \
        .withColumn('msg_lng_rad', F.radians(F.col('lon'))) \
        .drop('lat', 'lon') 
    
    return events_df

def events_geo(events_df, cities_df):
    window = Window().partitionBy('message_id').orderBy('distance')
    
    events_geo_df = events_df.crossJoin(cities_df) \
        .withColumn('distance',
            F.lit(2) * F.lit(6371) * F.asin(F.sqrt(
                F.pow(F.sin((F.col('msg_lat_rad') - F.col('lat_rad'))/F.lit(2)), 2)
                + F.cos(F.col("msg_lat_rad")) * F.cos(F.col("lat_rad")) 
                * F.pow(F.sin((F.col('msg_lng_rad') - F.col('lng_rad'))/F.lit(2)),2)
            ))
        ) \
        .drop("msg_lat_rad","msg_lng_rad","lat_rad", "lng_rad") \
        .withColumn('row_number', F.row_number().over(window)) \
        .filter(F.col('row_number') == 1) \
        .drop('row_number') \
        .withColumnRenamed("id", "city_id") \
        .persist()
    
    return events_geo_df

def actual_city(events_geo_df):
    window = Window().partitionBy('user_id').orderBy(F.col('date').desc())
    
    actual_city_df = events_geo_df \
        .selectExpr('user_id', 'message_id', 'date', 'city') \
        .withColumn('rn', F.row_number().over(window)) \
        .where('rn = 1') \
        .selectExpr('user_id', 'city as act_city') \
        .persist()
    
    return actual_city_df

def home_city(events_geo_df):
    window = Window().partitionBy('user_id', 'date', 'city').orderBy('message_id')
    window_2 = Window().partitionBy('user_id').orderBy('date', 'message_id')
    window_3 = Window().partitionBy('user_id').orderBy(F.col('date').desc(), F.col('message_id').desc())

    home_city_df = events_geo_df \
        .selectExpr('user_id', 'date', 'message_id', 'city') \
        .withColumn('rn', F.row_number().over(window)) \
        .where('rn = 1') \
        .drop('rn') \
        .withColumn('max_msg_date', F.max('date').over(Window().partitionBy('user_id'))) \
        .withColumn('prev_city', F.lag('city', offset = 1).over(window_2)) \
        .where('prev_city is null or city != prev_city') \
        .withColumn('dt_of_city_change', F.lead('date', offset = 1).over(window_2)) \
        .withColumn('days_in_city', 
                    F.when(
                        F.col('dt_of_city_change').isNotNull(), 
                        F.datediff(F.col('dt_of_city_change'), F.col('date'))
                        )
                    .otherwise(F.datediff(F.col('max_msg_date'), F.col('date')))
                ) \
        .where('days_in_city >= 1') \
        .withColumn('rn', F.row_number().over(window_3)) \
        .where('rn = 1') \
        .drop('rn') \
        .selectExpr('user_id', 'city as home_city') \
        .persist()
        
    return home_city_df

def travel_city(events_geo_df):
    window = Window.partitionBy('user_id').orderBy('date', 'message_id')
    
    travel_city_df = events_geo_df \
        .selectExpr('user_id', 'date', 'message_id', 'city') \
        .withColumn('prev_city_message', F.lag('city', offset=1).over(window)) \
        .where('city != prev_city_message') \
        .groupBy('user_id') \
        .agg(
            F.count('city').alias('travel_count'),
            F.collect_list('city').alias('travel_array')
            ) \
        .persist()
        
    return travel_city_df 

def time_info(events_geo_df):
    window = Window.partitionBy('user_id').orderBy(F.col('datetime').desc())
    
    time_info_df = events_geo_df \
        .select('user_id', 'date', 'datetime','message_id', 'city') \
        .where('datetime is not null') \
        .withColumn('rn', F.row_number().over(window)) \
        .where('rn = 1') \
        .withColumn('time', F.col('datetime').cast('Timestamp')) \
        .withColumn('timezone', F.concat(F.lit('Australia/'), F.col('city'))) \
        .selectExpr('user_id', 'time', 'timezone') 
        #.withColumn('local_time', F.from_utc_timestamp(F.col('time'), F.col('timezone')))
        
    return time_info_df

def mart_users(events_geo_df, actual_city_df, home_city_df, travel_city_df, time_info_df):
    mart_users_df = events_geo_df \
        .select('user_id') \
        .distinct() \
        .join(actual_city_df, 'user_id', 'left') \
        .join(home_city_df, 'user_id', 'left') \
        .join(travel_city_df, 'user_id', 'left') \
        .join(time_info_df, 'user_id', 'left') \
        .persist()

    return mart_users_df

def writer(df, output_path):
    return df \
        .coalesce(4) \
        .write \
        .mode('overwrite') \
        .parquet(f'{output_path}')

if __name__ == "__main__":
    main()