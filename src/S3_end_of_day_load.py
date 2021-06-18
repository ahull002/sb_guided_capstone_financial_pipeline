# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
# Import libraries
from pyspark.sql.window import Window
from datetime import date
import pyspark.sql.functions as F 

# 3.1 Populate trade dataset
# 3.1.1 Reading Trade partition dataset from its temporary location
trade_common = spark.read.parquet("/HdiNotebooks/output_dir_csv_json/partition=T/*.parquet")

# 3.1.2 Selecting the Necessary columns for Trade Records
trade = trade_common.select("trade_dt", "symbol", "exchange", "event_tm","event_seq_nb", "arrival_tm", "trade_pr")

# 3.1.3 Apply Data Correction
#In the exchange dataset, you can uniquely identify a record by the combination of trade_dt,
#symbol, exchange, event_tm, event_seq_nb. However, the exchange may correct an error in
#any submitted record by sending a new record with the same uniqueID. Such records will come with later arrival_tm. 

#Below code uses row_number and window partition and orderby to accept records with latest arrival_tme
trade_corrected=trade.withColumn("row_number",F.row_number().over(Window.partitionBy(trade.trade_dt,                   trade.symbol,trade.exchange,trade.event_tm,trade.event_seq_nb)                    .orderBy(trade.arrival_tm.desc()))).filter(F.col("row_number")==1).drop("row_number")

# 3.1.4 Writing the Trade Dataset back to Azure Storage
trade_date = "2020-07-29"
trade_corrected.coalesce(1).write.parquet("wasbs://guidedsparkpro-2021-02-16t15-01-46-464z@guidedsparkprhdistorage.blob.core.windows.net/trade/trade_dt={}".format(trade_date))


# 3.2 Populate quote dataset using the same method in 3.1
#########################################################
#########################################################

# 3.2.1 Reading the Quote partition dataset from its temporary location
quote_common = spark.read.parquet("/HdiNotebooks/output_dir_csv_json/partition=Q/*.parquet")

# 3.2.2 Selecting the Necessary columns for Trade Records
quote=quote_common.select("trade_dt","symbol","exchange","event_tm","event_seq_nb","arrival_tm","bid_pr","ask_pr")

# 3.2.3
#In the exchange dataset, you can uniquely identify a record by the combination of trade_dt,
#symbol, exchange, event_tm, event_seq_nb. However, the exchange may correct an error in
#any submitted record by sending a new record with the same uniqueID. Such records will come with later arrival_tm. 

#Below code uses row_number and window partition and orderby to accept records with latest arrival_tme

quote_corrected=quote.withColumn("row_number",F.row_number().over(Window.partitionBy(quote.trade_dt,quote.symbol,                                            quote.exchange,quote.event_tm,quote.event_seq_nb).                                            orderBy(quote.arrival_tm.desc()))).filter(F.col("row_number")==1).drop("row_number")

# 3.2.4 Writing the quote Dataset back to Azure Storage
### Writing back the Quote Dataset back to Azure Storage
trade_date = "2020-07-29"
quote.coalesce(1).write.parquet("wasbs://guidedsparkpro-2021-02-16t15-01-46-464z@guidedsparkprhdistorage.blob.core.windows.net/quote/trade_dt={}".format(trade_date))


