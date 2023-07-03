# Документация к проекту

### Файл geo.csv

К проету приложен файл `geo.csv`, в нём указаны координаты центра города.

```
# create directory for project
! hdfs dfs -mkdir /user/{username}/project_7/geo

! hdfs dfs -copyFromLocal /lessons/geo.csv /user/{username}/project_7/geo/
```

### Информация о событиях

Настройки сессии, чтобы проверить код:

```
import os
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'

import findspark
findspark.init()
findspark.find()

import pyspark
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.conf import SparkConf
sql = SparkSession \
    .builder \
    .master("yarn") \
    .config("spark.driver.cores", "2") \
    .config("spark.driver.memory", "1g") \
    .appName("data_lake_project") \
    .getOrCreate()
```

Был взят `sample(0.05)` для более оперативной обработке на ограниченных ресурсах.

```
df = sql.read.parquet("/user/master/data/geo/events/").sample(0.05)

df.repartition(12) \
    .write \
    .partitionBy("event_type") \
    .parquet("/user/{username}/data/events")
```
