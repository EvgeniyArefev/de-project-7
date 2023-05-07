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
    events_message_from_df = events_message_from(path_events, sql)
    events_message_to_df = events_message_to(path_events, sql)
    events_subscriptions_df = events_subscriptions(path_events, sql)
    events_union_sender_reciever_df = events_union_sender_reciever(path_events, sql)
    mart_firends_recomenadtion_df = mart_firends_recomenadtion(
        events_message_from_df, 
        events_message_to_df, 
        cities_df,
        events_union_sender_reciever_df,
        events_subscriptions_df
        )
    write = writer(mart_firends_recomenadtion_df, path_to_write)

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

def events_message_from(path, session):
    events_message_from_df = session.read.parquet(path) \
        .where('event_type = "message"') \
        .selectExpr(
            "event.message_id as message_id_from", 
            "event.message_from",     
            "event.subscription_channel",
            "lat", 
            "lon", 
            "date"
        ) \
        .withColumn('msg_lat_rad_from', F.col('lat') * F.lit(COEF_DEG_RAD)) \
        .withColumn('msg_lng_rad_from', F.col('lon') * F.lit(COEF_DEG_RAD)) \
        .where("message_from IS NOT NULL") \
        .drop('lat', 'lon') \
        .persist() 
    
    window = Window().partitionBy('message_from').orderBy(F.col('date').desc())

    events_message_from_df = events_message_from_df \
        .withColumn("row_number", F.row_number().over(window)) \
        .filter(F.col('row_number')==1) \
        .drop("row_number") \
        .persist()
    
    return events_message_from_df

def events_message_to(path, session):
    events_message_to_df = session.read.parquet(path) \
        .where('event_type = "message"') \
        .selectExpr(
            "event.message_id as message_id_to", 
            "event.message_to",     
            "event.subscription_channel",
            "lat", 
            "lon", 
            "date"
        ) \
        .withColumn('msg_lat_rad_to', F.col('lat') * F.lit(COEF_DEG_RAD)) \
        .withColumn('msg_lng_rad_to', F.col('lon') * F.lit(COEF_DEG_RAD)) \
        .where("message_to IS NOT NULL") \
        .drop('lat', 'lon') \
        .persist() 
    
    window = Window().partitionBy('message_to').orderBy(F.col('date').desc())

    events_message_to_df = events_message_to_df \
        .withColumn("row_number", F.row_number().over(window)) \
        .filter(F.col('row_number')==1) \
        .drop("row_number") \
        .persist()
    
    return events_message_to_df

def events_subscriptions(path, session):
    events_subscriptions_df = session.read.parquet(path) \
        .selectExpr(
            'event.user as user', 
            'event.subscription_channel as channel'
        ) \
        .where('user is not null and channel is not null') \
        .groupBy('user').agg(F.collect_list(F.col('channel')).alias('channels')) \
        .persist()
    
    return events_subscriptions_df

def events_union_sender_reciever(path_events, session):
    sender_reciever_df = session.read.parquet(path_events) \
        .selectExpr('event.message_from as sender','event.message_to as reciever') \
        .where('sender is not null and reciever is not null')
    
    reciever_sender_df = session.read.parquet(path_events) \
        .selectExpr('event.message_to as reciever','event.message_from as sender') \
        .where('sender is not null and reciever is not null')
    
    events_union_sender_reciever_df = sender_reciever_df \
        .union(reciever_sender_df) \
        .withColumn('sender_reciever', F.concat(F.col('sender'), F.lit("-") , F.col('reciever'))) \
        .drop('sender', 'reciever') \
        .distinct() \
    
    return events_union_sender_reciever_df

def mart_firends_recomenadtion(events_message_from_df, 
                               events_message_to_df, 
                               cities_df,
                               events_union_sender_reciever_df,
                               events_subscriptions_df
                              ):
    
    # Все возможные комбинации отправлителя и получателя
    mart_firends_recomenadtion_df = events_message_from_df.crossJoin(events_message_to_df) \
        .withColumn("distance", F.lit(2) * F.lit(6371) * F.asin(
            F.sqrt(
            F.pow(F.sin((F.col('msg_lat_rad_from') - F.col('msg_lat_rad_to'))/F.lit(2)),2)
            + F.cos(F.col("msg_lat_rad_from"))*F.cos(F.col("msg_lat_rad_to"))*
            F.pow(F.sin((F.col('msg_lng_rad_from') - F.col('msg_lng_rad_to'))/F.lit(2)),2)
            ))
        ) \
        .where("distance <= 1") \
        .withColumn("middle_point_lat_rad", (F.col('msg_lat_rad_from') + F.col('msg_lat_rad_to'))/F.lit(2)) \
        .withColumn("middle_point_lng_rad", (F.col('msg_lng_rad_from') + F.col('msg_lng_rad_to'))/F.lit(2)) \
        .selectExpr(
            "message_id_from as user_left", 
            "message_id_to as user_right",
            "middle_point_lat_rad", 
            "middle_point_lng_rad"
        ) \
        .distinct() \
        .persist()
    
    # Добавляем информацию по городу
    window = Window().partitionBy("user_left", "user_right").orderBy(F.col('distance').asc())

    mart_firends_recomenadtion_df = mart_firends_recomenadtion_df \
            .crossJoin(cities_df) \
            .withColumn("distance", F.lit(2) * F.lit(6371) * F.asin(
            F.sqrt(
                F.pow(F.sin((F.col('middle_point_lat_rad') - F.col('lat_rad'))/F.lit(2)),2)
                + F.cos(F.col("middle_point_lat_rad"))*F.cos(F.col("lat_rad"))*
                F.pow(F.sin((F.col('middle_point_lng_rad') - F.col('lng_rad'))/F.lit(2)),2)
                ))
            ) \
            .withColumn("row_number", F.row_number().over(window)) \
            .filter(F.col('row_number') == 1) \
            .withColumn("timezone",F.concat(F.lit("Australia/"),F.col('city'))) \
            .withColumnRenamed("city", "zone_id") \
            .withColumn('sender_reciever_combine', F.concat(F.col('user_left'), F.lit('-'), F.col('user_right'))) \
            .select("user_left", "user_right", "id", "zone_id", "distance", 'sender_reciever_combine')
    
    # Убираем тех, кто уже общался друг с другом
    mart_firends_recomenadtion_df = mart_firends_recomenadtion_df \
        .join(
            events_union_sender_reciever_df,
            mart_firends_recomenadtion_df.sender_reciever_combine == events_union_sender_reciever_df.sender_reciever,
            'leftanti'
        )
    
    # Рекомендации пользователям
    mart_firends_recomenadtion_df = mart_firends_recomenadtion_df \
        .join(events_subscriptions_df, 
              mart_firends_recomenadtion_df.user_left == events_subscriptions_df.user,
             'left'
        ) \
        .withColumnRenamed('channels', 'channels_left') \
        .drop('user') \
        .join(events_subscriptions_df, 
              mart_firends_recomenadtion_df.user_right == events_subscriptions_df.user,
             'left'
        ) \
        .withColumnRenamed('channels', 'channels_right') \
        .drop('user') \
        .withColumn('intersect_channels', F.array_intersect(F.col('channels_left'), F.col('channels_right'))) \
        .filter(F.size(F.col("intersect_channels")) >= 1) \
        .where('user_left <> user_right') \
        .withColumn("processed_dttm", F.current_timestamp()) \
        .withColumn("timezone",F.concat(F.lit("Australia/"),F.col('zone_id'))) \
        .select('user_left', 'user_right', 'processed_dttm', 'zone_id') \
        .persist()
            
    return mart_firends_recomenadtion_df

def writer(df, output_path):
    return df \
        .write \
        .mode('overwrite') \
        .parquet(output_path)


if __name__ == "__main__":
    main()