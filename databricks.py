
if __name__ == '__main__':
    raise Exception('This script is not meant to be run as is. Please copy and paste contents into a databricks notebook')

# Fill in :
kinesisRegion =
kinesisStreamName =
bucket_name =
csv_name = 'test.csv' #Should match the name in config.py

!pip install textblob
!pip install s3fs
from pyspark.sql.functions import UserDefinedFunction, col, get_json_object, regexp_extract, window, to_timestamp, asc,desc
from pyspark.sql.types import StringType, FloatType
import json
import textblob
import textblob
import os
import s3fs
spark.conf.set("spark.sql.shuffle.partitions",2) # For databricks community edition
os.environ['AWS_ACCESS_KEY_ID'] =awsAccessKeyId
os.environ['AWS_SECRET_ACCESS_KEY'] = awsSecretKey
os.environ['AWS_DEFAULT_REGION']  = kinesisRegion



kinesisDF = spark \
  .readStream \
  .format("kinesis") \
  .option("streamName", kinesisStreamName)\
  .option("region", kinesisRegion) \
  .option("initialPosition", "LATEST") \
  .option("format", "json") \
  .option("awsAccessKey", awsAccessKeyId)\
  .option("awsSecretKey", awsSecretKey) \
  .option("inferSchema", "true") \
  .load()



def get_sentiment(text):
    from textblob import TextBlob
    try:
      tweet = TextBlob(text)
      return tweet.sentiment.polarity
    except:
      return None

# Define your function
getSentiment = UserDefinedFunction(get_sentiment, StringType())
# Apply the UDF using withColumn

outDF = (kinesisDF
       .selectExpr("cast(data as string)")
       .withColumn('id', get_json_object(col("data"),"$[0].id"))
       .withColumn('ts', get_json_object(col("data"),"$[0].ts"))
       .withColumn('tweet', get_json_object(col("data"),"$[0].tweet"))
       .withColumn('tag', get_json_object(col("data"),"$[0].tag"))
       .withColumn('sentiment', getSentiment(col("tweet")).cast(FloatType()))
       .withColumn('datetime',
                 to_timestamp(regexp_extract( col('ts'), '\\w\\w\\w \\d\\d? (\\d+):(\\d+):(\\d+)',0), "MMM dd HH:mm:ss"))
       .select(col('sentiment'),col('datetime'), col('tag'), col('tweet'))
       .withColumn('sentiment_cnt', col("sentiment"))
       .withColumn('tweet_dup', col("tweet"))
       .groupBy(col("tag"), window(col('datetime'), "5 seconds").alias("timewindow"))
       .agg({'sentiment_cnt': 'count','sentiment':'avg','tweet':'first','tweet_dup':'last'})
       .withColumnRenamed("count(sentiment_cnt)","notweets")
       .withColumnRenamed("avg(sentiment)","avgsentiment")
       .withColumnRenamed('first(tweet)',"tweet1")
       .withColumnRenamed('last(tweet_dup)',"tweet2")
       .withColumn('timestamp',col("timewindow").cast("string"))
       .drop(col("timewindow"))
       )


def writeFunc(batch, batch_id):
  df = batch.toPandas().set_index(['timestamp','tag'])
  try:
    df_old = pd.read_csv('s3a://{}/{}'.format(bucket_name, csv_name),
        index_col='timestamp', delimiter=';').set_index('tag', append=True)
    merged = pd.merge(df,df_old,how='outer',left_index=True,right_index=True)
    merged = merged.reindex(sorted(merged.columns), axis=1)
    df = merged.fillna(method='backfill',axis=1)\
          .rename({'avgsentiment_x':'avgsentiment',
                   'notweets_x':'notweets',
                   'tweet1_x':'tweet1',
                   'tweet2_x':'tweet2' },axis=1)[['avgsentiment','notweets','tweet1','tweet2']]\
          .reset_index().set_index('timestamp')
  except Exception:
    pass
  df.to_csv('s3a://{}/{}'.format(bucket_name, csv_name),sep=';')
