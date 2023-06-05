import sys

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
from pyspark.sql.window import Window 

def main():
    path_events = sys.argv[1]
    path_geo_cities = sys.argv[2]
    path_to_write = sys.argv[3]

    conf = SparkConf().setAppName('datamart_users')
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)

    cities_df = cities(path_geo_cities, sql)
    events_df = events(path_events, sql)
    user_subscription_channels_df = user_subscription_channels(events_df)
    user_message_from_df = user_message_from(events_df, cities_df)
    combinations_sender_receiver_df = combinations_sender_receiver(events_df)
    mart_friends_recomendation_df = mart_friends_recomendation(user_message_from_df, 
                                                               user_subscription_channels_df, 
                                                               combinations_sender_receiver_df, 
                                                               cities_df)
    write = writer(mart_friends_recomendation_df, path_to_write)

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
        .selectExpr('date',
                    'event_type',                    
                    'event.user as user_id',
                    'event.subscription_channel',
                    'event.message_id',
                    'event.message_from as user_msg_from',
                    'event.message_to as user_msg_to',
                    'lat', 
                    'lon') \
        .persist()
    
    return events_df

def user_subscription_channels(events_df):
    user_subscription_channels_df = events_df \
        .selectExpr('user_id', 'subscription_channel') \
        .where('user_id is not null and subscription_channel is not null') \
        .persist()

    return user_subscription_channels_df

def user_message_from(events_df, cities_df):
    window = Window().partitionBy('user_msg_from').orderBy(F.col('date').desc(), F.col('message_id').desc())
    window_2 = Window().partitionBy('user_msg_from').orderBy('distance')

    user_message_from_df = events_df \
        .where("event_type = 'message'") \
        .selectExpr('user_msg_from', 'message_id', 'date', 'lat', 'lon') \
        .withColumn('lat_msg_from', F.radians(F.col('lat'))) \
        .withColumn('lng_msg_from', F.radians(F.col('lon'))) \
        .withColumn('rn', F.row_number().over(window)) \
        .where('rn = 1') \
        .select('user_msg_from', 'lat_msg_from', 'lng_msg_from') \
        .crossJoin(cities_df) \
        .withColumn('distance',
            F.lit(2) * F.lit(6371) * F.asin(F.sqrt(
                F.pow(F.sin((F.col('lat_msg_from') - F.col('lat_rad'))/F.lit(2)), 2)
                + F.cos(F.col("lat_msg_from")) * F.cos(F.col("lat_rad")) 
                * F.pow(F.sin((F.col('lng_msg_from') - F.col('lng_rad'))/F.lit(2)),2)
            ))
        ) \
        .drop("lat_rad","lng_rad") \
        .withColumn('rn', F.row_number().over(window_2)) \
        .where('rn = 1') \
        .drop('rn', 'distance') \
        .withColumnRenamed("id", "zone_id") \
        .persist()
      
    return user_message_from_df

def combinations_sender_receiver(events_df):
    combinations_sender_receiver_df = events_df \
        .where("event_type = 'message'") \
        .selectExpr('user_msg_from as user_left','user_msg_to as user_right') \
        .union(
            events_df \
            .where("event_type = 'message'") \
            .selectExpr('user_msg_to as user_left','user_msg_from as user_right')
        ) \
        .where('user_left is not null and user_right is not null') \
        .distinct() \
        .withColumn('users_comb', F.trim(F.concat(F.col('user_left'), F.lit('-'), F.col('user_right')))) \
        .drop('user_left', 'user_right') \
        .persist()
    
    return combinations_sender_receiver_df

def mart_friends_recomendation(user_message_from_df, 
                               user_subscription_channels_df,
                               combinations_sender_receiver_df, 
                               cities_df):
    
    #пользователи подписаны на один канал
    mart_friends_recomendation_df = user_subscription_channels_df \
        .withColumnRenamed('user_id', 'user_left') \
        .join(user_subscription_channels_df.withColumnRenamed('user_id', 'user_right'), 
            'subscription_channel', 
            'inner') \
        .where('user_left <> user_right') \
        .drop('subscription_channel')
    
    #оставляем пользователей, которые ранее никогда не переписывались
    mart_friends_recomendation_df = mart_friends_recomendation_df \
        .withColumn('users_comb', F.trim(F.concat(F.col('user_left'), F.lit('-'), F.col('user_right')))) \
        .join(combinations_sender_receiver_df, 'users_comb', 'leftanti') \
        .drop('users_comb')
    
    #расстояние между пользователями не превышает 1 км
    mart_friends_recomendation_df = mart_friends_recomendation_df \
        .join(user_message_from_df, 
            mart_friends_recomendation_df.user_left == user_message_from_df.user_msg_from, 
            'left') \
        .drop('user_msg_from') \
        .withColumnRenamed('lat_msg_from', 'user_left_lat') \
        .withColumnRenamed('lng_msg_from', 'user_left_lng') \
        .withColumnRenamed('zone_id', 'zone_id_left') \
        .withColumnRenamed('city', 'city_left') \
        .join(user_message_from_df, 
            mart_friends_recomendation_df.user_right == user_message_from_df.user_msg_from, 
            'left') \
        .drop('user_msg_from', 'zone_id', 'city') \
        .withColumnRenamed('lat_msg_from', 'user_right_lat') \
        .withColumnRenamed('lng_msg_from', 'user_right_lng') \
        .withColumn('distance_between_users',
                F.lit(2) * F.lit(6371) * F.asin(
                    F.sqrt(
                        F.pow(F.sin((F.col('user_left_lat') - F.col('user_right_lat')) / F.lit(2)), 2)
                        + F.cos(F.col("user_left_lat")) * F.cos(F.col("user_right_lat")) 
                        * F.pow(F.sin((F.col('user_left_lng') - F.col('user_right_lng')) / F.lit(2)),2)
                    )
                )
            ) \
        .where('distance_between_users <= 1') \
        .drop('user_left_lat', 'user_left_lng', 'user_right_lat', 'user_right_lng', 'distance_between_users')
    
    #выводим нужные для аналитики поля
    mart_friends_recomendation_df = mart_friends_recomendation_df \
        .withColumn('current_dttm', F.current_timestamp()) \
        .withColumn('timezone', F.concat(F.lit('Australia/'), F.col('city_left'))) \
        .selectExpr('user_left', 
                    'user_right', 
                    'current_dttm as processed_dttm', 
                    'city_left as zone_id') \
        .persist()
        
    return mart_friends_recomendation_df

def writer(df, output_path):
    return df \
        .coalesce(4) \
        .write \
        .mode('overwrite') \
        .parquet(f'{output_path}')

if __name__ == "__main__":
    main()