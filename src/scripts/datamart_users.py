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
    actual_geo_df = actual_geo(events_geo_df)
    home_geo_df = home_geo(travel_geo_df)
    travel_geo_df = travel_geo(events_geo_df)
    mart_actual_home_df = mart_actual_home(actual_geo_df, home_geo_df, cities_df)
    mart_users_cities_df = mart_users_cities(path_events, mart_actual_home_df, sql)
    write = writer(mart_users_cities_df, path_to_write)  

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
        .where('event_type = "message"') \
        .select("event.message_id", "event.message_from", "lat", "lon", "date") \
        .withColumn('msg_lat_rad', F.col('lat') * F.lit(COEF_DEG_RAD)) \
        .withColumn('msg_lng_rad', F.col('lon') * F.lit(COEF_DEG_RAD)) \
        .drop('lat', 'lon') \
        .persist()
    
    return events_df

def events_geo(events_df, cities_df):
    events_geo_df = events_df.crossJoin(cities_df) \
        .withColumn('distance',
            F.lit(2) * F.lit(6371) * F.asin(F.sqrt(
                F.pow(F.sin((F.col('msg_lat_rad') - F.col('lat_rad'))/F.lit(2)), 2)
                + F.cos(F.col("msg_lat_rad")) 
                * F.cos(F.col("lat_rad")) 
                * F.pow(F.sin((F.col('msg_lng_rad') - F.col('lng_rad'))/F.lit(2)),2)
            ))
        ) \
        .drop("msg_lat_rad","msg_lng_rad","lat_rad", "lng_rad")

    window = Window().partitionBy('message_id').orderBy('distance')
    events_geo_df = events_geo_df.withColumn('row_number', F.row_number().over(window)) \
        .filter(F.col('row_number') == 1) \
        .drop('row_number') \
        .withColumnRenamed("id", "city_id") \
        .persist()
    
    return events_geo_df

def actual_geo(input_df):
    window = Window().partitionBy('message_from').orderBy(F.col('date').desc())
    actual_geo_df = input_df \
        .withColumn('row_number', F.row_number().over(window)) \
        .filter(F.col('row_number') == 1) \
        .selectExpr('message_id', 'user_id', 'city', 'city_id')

    return actual_geo_df

def travel_geo(input_df):
    window = Window().partitionBy('user_id', 'message_id').orderBy(F.col('date'))
    travel_geo_df = input_df \
        .withColumn('dense_rank', F.dense_rank().over(window)) \
        .withColumn('date_diff', F.datediff(
            F.col('date').cast(DateType()), 
            F.to_date(F.col('dense_rank').cast('string'), 'd')
            )
        ) \
        .select('date_diff', 'user_id', 'date', 'message_id', 'city_id') \
        .groupBy('user_id', 'date_diff', 'city_id') \
        .agg(F.countDistinct(F.col('date')).alias('cnt_city')) 
 
    return travel_geo_df

def home_geo(input_df):
    home_geo_df = input_df \
        .filter((F.col('cnt_city') >= 1)) \
        .withColumn('max_dt', F.max(F.col('date_diff')).over(Window().partitionBy('user_id'))) \
        .filter(F.col('date_diff') == F.col('max_dt')) \
        .persist()
    
    return home_geo_df

def mart_actual_home(actual_city, home_city, cities_df):
    cities_df = cities_df.select("id", "city")
    home_city = home_city \
        .join(cities_df, home_city.city_id == cities_df.id, "inner") \
        .selectExpr('user_id', 'city as home_city')
    mart_actual_home_df = actual_city \
        .join(home_city, actual_city.user_id == home_city.user_id, "fullouter") \
        .select(
            F.coalesce(actual_geo_df.user_id, home_city.user_id).alias('user_id'),
            F.col('city').alias('act_city'),
            'home_city'
        )
    
    return mart_actual_home_df

def mart_users_cities(events_path, mart_actual_home, session):
    times = session \
        .read.parquet(events_path) \
        .selectExpr("event.message_from as user_id", "event.datetime", "event.message_id") \
        .where("datetime IS NOT NULL")

    window = Window().partitionBy('user_id').orderBy(F.col('datetime').desc())
    times_w = times \
        .withColumn("row_number", F.row_number().over(window)) \
        .filter(F.col('row_number')==1) \
        .withColumn("Time",F.col("datetime").cast("Timestamp")) \
        .selectExpr("user_id as user", "Time")
    
    mart_df = mart_actual_home \
        .join(times_w, mart_actual_home.user_id == times_w.user, "left") \
        .drop("user") \
        .withColumn("timezone",F.concat(F.lit("Australia/"),F.col('act_city')))
  
    return mart_df

def writer(df, output_path):
    return df \
        .write \
        .mode('overwrite') \
        .parquet(output_path)

if __name__ == "__main__":
    main()
