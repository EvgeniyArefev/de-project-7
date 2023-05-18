import sys

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
from pyspark.sql.window import Window 
from pyspark.sql.types import DateType

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
        .withColumn('lat_rad', F.regexp_replace('lat', ',', '.') * F.lit(COEF_DEG_RAD)) \
        .withColumn('lng_rad', F.regexp_replace('lng', ',', '.') * F.lit(COEF_DEG_RAD)) \
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
        .withColumn('msg_lat_rad', F.col('lat') * F.lit(COEF_DEG_RAD)) \
        .withColumn('msg_lng_rad', F.col('lon') * F.lit(COEF_DEG_RAD)) \
        .drop('lat', 'lon') 
    
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
    window_lag = Window().partitionBy('user_id', 'city').orderBy(F.col('date'))
    window_rn = Window().partitionBy('user_id', 'city', 'ddif').orderBy(F.col('date'))
    window_max = Window.partitionBy('user_id')
    
    home_city_df = events_geo_df \
        .selectExpr('user_id', 'date', 'city') \
        .distinct() \
        .withColumn('lag', F.lag('date', offset=1, default=None).over(window_lag)) \
        .withColumn('ddif', F.datediff(F.col('date').cast(DateType()), F.col('lag').cast(DateType()))) \
        .withColumn('ddif', F.when(F.col('ddif').isNull(), 1).otherwise(F.col('ddif'))) \
        .withColumn('rn', F.row_number().over(window_rn)) \
        .where('rn >= 10') \
        .selectExpr('user_id', 'date', 'city', 'rn') \
        .withColumn('max_date', F.max('date').over(window_max)) \
        .withColumn('max_rn', F.max('rn').over(window_max)) \
        .where('date = max_date and rn = max_rn') \
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
        .write \
        .mode('overwrite') \
        .parquet(f'{output_path}')

if __name__ == "__main__":
    main()