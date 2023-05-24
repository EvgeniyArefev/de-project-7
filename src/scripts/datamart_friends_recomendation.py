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
    user_subscription_channels_df = user_subscription_channels(path_events, sql)
    user_message_from_df = user_message_from(path_events, sql, user_subscription_channels_df)
    user_message_to_df = user_message_to(path_events, sql, user_subscription_channels_df)
    combinations_sender_receiver_df = combinations_sender_receiver(path_events, sql)
    mart_friends_recomendation_df = mart_friends_recomendation(user_message_from_df, 
                                                               user_message_to_df, 
                                                               combinations_sender_receiver_df, 
                                                               cities_df)

    write = writer(mart_friends_recomendation_df, path_to_write)

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

def user_subscription_channels(path, session):
    user_subscription_channels_df = session.read.parquet(f'{path}') \
        .selectExpr('event.user as user_id', 'event.subscription_channel') \
        .where('user_id is not null and subscription_channel is not null') \
        .groupBy('user_id') \
        .agg(F.collect_list('subscription_channel').alias('sub_channels')) \
        .persist()
        
    
    return user_subscription_channels_df

def user_message_from(path, session, df_with_sub_channels):
    window = Window().partitionBy('user_msg_from').orderBy(F.col('date').desc(), F.col('message_id').desc())
    
    user_message_from_df = session.read.parquet(f'{path}/event_type=message') \
        .selectExpr('event.message_from as user_msg_from', 'event.message_id', 'date', 'lat', 'lon') \
        .withColumn('lat_msg_from', F.col('lat') * F.lit(COEF_DEG_RAD)) \
        .withColumn('lng_msg_from', F.col('lon') * F.lit(COEF_DEG_RAD)) \
        .withColumn('rn', F.row_number().over(window)) \
        .where('rn = 1') \
        .select('user_msg_from', 'lat_msg_from', 'lng_msg_from')
    
    user_message_from_df = user_message_from_df \
        .join(df_with_sub_channels, 
              user_message_from_df.user_msg_from == df_with_sub_channels.user_id, 
              'left') \
        .withColumnRenamed('sub_channels', 'sub_channels_user_from') \
        .drop('user_id') \
        .persist()
        

    return user_message_from_df

def user_message_to(path, session, df_with_sub_channels):
    window = Window().partitionBy('user_msg_to').orderBy(F.col('date').desc(), F.col('message_id').desc())
    
    user_message_to_df = session.read.parquet(f'{path}/event_type=message') \
        .selectExpr('event.message_to as user_msg_to', 'event.message_id', 'date', 'lat', 'lon') \
        .withColumn('lat_msg_to', F.col('lat') * F.lit(COEF_DEG_RAD)) \
        .withColumn('lng_msg_to', F.col('lon') * F.lit(COEF_DEG_RAD)) \
        .withColumn('rn', F.row_number().over(window)) \
        .where('rn = 1') \
        .select('user_msg_to', 'lat_msg_to', 'lng_msg_to')
    
    user_message_to_df = user_message_to_df \
        .join(df_with_sub_channels, 
              user_message_to_df.user_msg_to == df_with_sub_channels.user_id, 
              'left') \
        .withColumnRenamed('sub_channels', 'sub_channels_user_to') \
        .drop('user_id') \
        .persist()

    return user_message_to_df

def combinations_sender_receiver(path, session):
    combinations_sender_receiver_df = session.read.parquet(f'{path}/event_type=message') \
        .selectExpr('event.message_from as user_right','event.message_to as user_left') \
        .union(
            session.read.parquet(f'{path}/event_type=message')
            .selectExpr('event.message_to as user_right','event.message_from as user_left')
        ) \
        .where('user_right is not null and user_left is not null') \
        .distinct() \
        .withColumn('users_comb', F.trim(F.concat(F.col('user_right'), F.lit('-'), F.col('user_left')))) \
        .drop('user_right', 'user_left') \
        .persist()
    
    return combinations_sender_receiver_df

def mart_friends_recomendation(user_message_from_df, 
                               user_message_to_df, 
                               combinations_sender_receiver_df, 
                               cities_df):
    
    #пользователи подписаны на один канал
    mart_friends_recomendation_df = user_message_from_df \
        .crossJoin(user_message_to_df) \
        .withColumn('inter_channels', F.array_intersect(F.col('sub_channels_user_from'), F.col('sub_channels_user_to'))) \
        .drop('sub_channels_user_from', 'sub_channels_user_to') \
        .filter(F.size(F.col('inter_channels'))>=1) \
        .drop('inter_channels')
    
     #оставляем пользователей, которые ранее никогда не переписывались
    mart_friends_recomendation_df = mart_friends_recomendation_df \
        .withColumn('users_comb', F.trim(F.concat(F.col('user_msg_from'), F.lit('-'), F.col('user_msg_to')))) \
        .join(combinations_sender_receiver_df, 'users_comb', 'leftanti')
    
    #расстояние между пользователями не превышает 1 км
    mart_friends_recomendation_df = mart_friends_recomendation_df \
        .withColumn('distance_between_users',
            F.lit(2) * F.lit(6371) * F.asin(
                F.sqrt(
                    F.pow(F.sin((F.col('lat_msg_from') - F.col('lat_msg_to')) / F.lit(2)), 2)
                    + F.cos(F.col("lat_msg_from")) * F.cos(F.col("lat_msg_to")) 
                    * F.pow(F.sin((F.col('lng_msg_from') - F.col('lng_msg_to')) / F.lit(2)),2)
                )
            )
        ) \
        .where('distance_between_users <= 1') \
        .drop('distance_between_users')
    
     #уточняем идентификатор зоны (города)
    window = Window().partitionBy('users_comb').orderBy(F.col('distance').desc())
    
    mart_friends_recomendation_df = mart_friends_recomendation_df \
        .withColumn('middle_point_lat_rad', (F.col('lat_msg_from') + F.col('lat_msg_to'))/F.lit(2)) \
        .withColumn('middle_point_lng_rad', (F.col('lng_msg_from') + F.col('lng_msg_to'))/F.lit(2)) \
        .drop('lat_msg_from', 'lng_msg_from', 'lat_msg_to', 'lng_msg_to') \
        .crossJoin(cities_df) \
        .withColumn('distance',
            F.lit(2) * F.lit(6371) * F.asin(F.sqrt(
                F.pow(F.sin((F.col('middle_point_lat_rad') - F.col('lat_rad'))/F.lit(2)), 2)
                + F.cos(F.col("middle_point_lat_rad")) * F.cos(F.col("lat_rad")) 
                * F.pow(F.sin((F.col('middle_point_lng_rad') - F.col('lng_rad'))/F.lit(2)),2)
            ))
        ) \
        .withColumn('rn', F.row_number().over(window)) \
        .where('rn = 1')
    
    #выводим нужные для аналитики поля
    #не вывожу local_time так как может выдавать ошибку, если таймзоны нет в функции from_utc_timestamp
    mart_friends_recomendation_df = mart_friends_recomendation_df\
        .withColumn('current_dttm', F.current_timestamp()) \
        .withColumn('timezone', F.concat(F.lit('Australia/'), F.col('city'))) \
        .selectExpr('user_msg_from as user_left', 
                    'user_msg_to as user_right', 
                    'current_dttm as processed_dttm', 
                    'city as zone_id') \
        .persist()
        
    return mart_friends_recomendation_df

def writer(df, output_path):
    return df \
        .write \
        .mode('overwrite') \
        .parquet(f'{output_path}')

if __name__ == "__main__":
    main()