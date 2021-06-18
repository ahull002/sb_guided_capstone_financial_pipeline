# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
import pyspark
from pyspark.sql import SparkSession


# %%
# Establish connection
storageAccountName = 'storage1ngest'
storageAccountAccessKey = 'something_goes_here=='
blobContainerName = 'main'


if not any(mount.mountPoint == '/mnt/FileStore/MountFolder/' for mount in dbutils.fs.mounts()):
  try:
    dbutils.fs.mount(
    source = "wasbs://{}@{}.blob.core.windows.net".format(blobContainerName, storageAccountName),
    mount_point = "/mnt/FileStore/MountFolder/",
    extra_configs = {'fs.azure.account.key.' + storageAccountName + '.blob.core.windows.net': storageAccountAccessKey}
  )
  except Exception as e:
    print("already mounted. Try to unmount first")

display(dbutils.fs.ls("dbfs:/mnt/FileStore/MountFolder/data"))


# %%
# Un-Mounter: Connection script
#Only use this to unmount container
#dbutils.fs.unmount("/mnt/mounter1")


# %%
# CSV PARSER
# Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType ,LongType
from pyspark.sql.types import ArrayType, DoubleType, BooleanType, DateType, TimestampType, DecimalType

import datetime
from typing import List
from decimal import Decimal


# Parse .csv files
def parse_csv(line:str):
    """
      Function to parse comma seperated records
      
    """        
    try:
        # position of the record_type field
        record_type_pos = 2

        # get the common fields applicable to both 'Q' and 'T' type records
        record = line.split(",")
        trade_dt= datetime.datetime.strptime(record[0], '%Y-%m-%d')
        arrival_tm=datetime.datetime.strptime(record[1], '%Y-%m-%d %H:%M:%S.%f')
        rec_type=record[2]
        symbol=record[3]
        exchange=record[6]
        event_tm=datetime.datetime.strptime(record[4], '%Y-%m-%d %H:%M:%S.%f')
        event_seq_nb=int(record[5])
        
        # fields specific to record_type is 'T'
        if record[record_type_pos] == "T":
            trade_pr=Decimal(record[7])
            event = (trade_dt,rec_type,symbol,exchange,event_tm,event_seq_nb,arrival_tm,                     trade_pr,Decimal('0.0'),0,Decimal('0.0'),0,"T")
            return event
        
        # fields specific to record_type is 'Q'
        elif record[record_type_pos] == "Q":
            bid_pr=Decimal(record[7])
            bid_size=int(record[8])
            ask_pr=Decimal(record[9])
            ask_size=int(record[10])

            event = (trade_dt,rec_type,symbol,exchange,event_tm,event_seq_nb,arrival_tm,                     Decimal('0.0'),bid_pr,bid_size,ask_pr,ask_size,"Q")
            return event
    # capture record in a bad partition if any error occurs
    except Exception as e:
      return ("","","","","","","","","","","","B",line)
        
# schema to parse both Q and T type records
common_event = StructType()               .add("trade_dt",DateType(),True)               .add("rec_type",StringType(),True)               .add("symbol",StringType(),True)               .add("exchange",StringType(),True)               .add("event_tm",TimestampType(),True)               .add("event_seq_nb",IntegerType(),True)               .add("arrival_tm",TimestampType(),True)               .add("trade_pr",DecimalType(17,14),True)               .add("bid_pr",DecimalType(17,14),True)               .add("bid_size",IntegerType(),True)               .add("ask_pr",DecimalType(17,14),True)               .add("ask_size",IntegerType(),True)               .add("partition",StringType(),True)
              
## Spark to process the source data (below line is for local execution)
spark = SparkSession.builder.master('local').        appName('app').getOrCreate()


raw = spark.sparkContext.           textFile("dbfs:/mnt/FileStore/MountFolder/data/csv/*/NYSE/*.txt")

# Parse the text file and parse using the parse_csv function to get the rdd in proper format. 
parsed = raw.map(lambda line: parse_csv(line))
data_csv = spark.createDataFrame(parsed,schema=common_event)

# Save the final dataframe as parquet files in partitions
data_csv.write.partitionBy("partition").mode("overwrite").parquet("dbfs:/mnt/FileStore/MountFolder/output_dir")


# %%
# Display .csv dataframe after parsing
data_csv.show(10, truncate=False)


# %%
# Save .csv dataframe after parsing as a parquet file
data_csv.write.partitionBy("partition").mode("overwrite").parquet("dbfs:/mnt/FileStore/MountFolder/output_dir")


# %%
# Display status
display(dbutils.fs.ls("dbfs:/mnt/FileStore/MountFolder/output_dir"))


# %%
# Display status of Parquet file save
display(dbutils.fs.ls("dbfs:/mnt/FileStore/MountFolder/output_dir/partition=Q/"))


# %%
# JSON PARSER
# Import libraries
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType ,LongType
from pyspark.sql.types import ArrayType, DoubleType, BooleanType, DateType, TimestampType, DecimalType

import json
from decimal import Decimal 
import datetime

def parse_json(line:str):
    """
      Function to parse json records
      
    """
    try:
        # built-in function to parse the json file
        record = json.loads(line)
        
        # Common fields applicable to both 'Q' and 'T' type records
        record_type = record['event_type']
        trade_dt= datetime.datetime.strptime(record['trade_dt'], '%Y-%m-%d')
        arrival_tm=datetime.datetime.strptime(record['file_tm'], '%Y-%m-%d %H:%M:%S.%f')
        rec_type=record_type
        symbol=record['symbol']
        exchange=record['exchange']
        event_tm=datetime.datetime.strptime(record['event_tm'], '%Y-%m-%d %H:%M:%S.%f')
        event_seq_nb=int(record['event_seq_nb'])

        # capturing field values specific to "T" type records
        if record_type == "T":
            trade_pr=Decimal(record['price'])
            event = (trade_dt,rec_type,symbol,exchange,event_tm,event_seq_nb,arrival_tm,                     trade_pr,Decimal('0.0'),0,Decimal('0.0'),0,"T")
            return event
        
        # capturing field values specific to "Q" type records
        elif record_type == "Q":
            bid_pr=Decimal(record['bid_pr'])
            bid_size=int(record['bid_size'])
            ask_pr=Decimal(record['ask_pr'])
            ask_size=int(record['ask_size'])
            event = (trade_dt,rec_type,symbol,exchange,event_tm,event_seq_nb,arrival_tm,                     Decimal('0.0'),bid_pr,bid_size,ask_pr,ask_size,"Q")
            return event

    # capture record in a bad partition if any error occurs
    except Exception as e:
            return ("","","","","","","","","","","","B",line)



## Spark to process the source data (below line is for local execution)
spark = SparkSession.builder.master('local').appName('app').getOrCreate()

# schema to parse both Q and T type records
common_event = StructType()               .add("trade_dt",DateType(),True)               .add("rec_type",StringType(),True)               .add("symbol",StringType(),True)               .add("exchange",StringType(),True)               .add("event_tm",TimestampType(),True)               .add("event_seq_nb",IntegerType(),True)               .add("arrival_tm",TimestampType(),True)               .add("trade_pr",DecimalType(17,14),True)               .add("bid_pr",DecimalType(17,14),True)               .add("bid_size",IntegerType(),True)               .add("ask_pr",DecimalType(17,14),True)               .add("ask_size",IntegerType(),True)               .add("partition",StringType(),True)


raw = spark.sparkContext.textFile("dbfs:/mnt/FileStore/MountFolder/data/json/*/NASDAQ/*.txt")
parsed = raw.map(lambda line: parse_json(line))
data_json = spark.createDataFrame(parsed,schema=common_event)

# Save the final dataframe as parquet files in partitions
data_json.write.partitionBy("partition").mode("overwrite").parquet("dbfs:/mnt/FileStore/MountFolder/output_dir_json")


# %%
# Display .json file in a dataframe
data_json.show(10,truncate=False)


# %%
# Save .json dataframe after parsing as a parquet file
data_json.write.partitionBy("partition").mode("overwrite").parquet("dbfs:/mnt/FileStore/MountFolder/output_dir_json")


# %%
# Display status of .json file write
display(dbutils.fs.ls("dbfs:/mnt/FileStore/MountFolder/output_dir_json"))


# %%
# Display status of Parquet file save
display(dbutils.fs.ls("dbfs:/mnt/FileStore/MountFolder/output_dir_json/partition=Q/"))


# %%



