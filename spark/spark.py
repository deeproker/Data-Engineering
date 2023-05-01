from pyspark.sql import SparkSession
import pyspark.sql.types as T
from pyspark.sql.functions import first, last, coalesce
from pyspark.sql.functions import  last, struct, when ,expr,col
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.functions import sum, when, col

#print('hello')



def load_trades(spark):
    data = [
        (10, 1546300800000, 37.50, 100.000),
        (10, 1546300801000, 37.51, 100.000),
        (20, 1546300804000, 12.67, 300.000),
        (10, 1546300807000, 37.50, 200.000),
    ]
    schema = T.StructType(
        [
            T.StructField("id", T.LongType()),
            T.StructField("timestamp", T.LongType()),
            T.StructField("price", T.DoubleType()),
            T.StructField("quantity", T.DoubleType()),
        ]
    )

    return spark.createDataFrame(data, schema)


def load_prices(spark):
    data = [
        (10, 1546300799000, 37.50, 37.51),
        (10, 1546300802000, 37.51, 37.52),
        (10, 1546300806000, 37.50, 37.51),
    ]
    schema = T.StructType(
        [
            T.StructField("id", T.LongType()),
            T.StructField("timestamp", T.LongType()),
            T.StructField("bid", T.DoubleType()),
            T.StructField("ask", T.DoubleType()),
        ]
    )

    return spark.createDataFrame(data, schema)


def fill(trades, prices):
    """
    Combine the sets of events and fill forward the value columns so that each
    row has the most recent non-null value for the corresponding id. For
    example, given the above input tables the expected output is:

    +---+-------------+-----+-----+-----+--------+
    | id|    timestamp|  bid|  ask|price|quantity|
    +---+-------------+-----+-----+-----+--------+
    | 10|1546300799000| 37.5|37.51| null|    null|
    | 10|1546300800000| 37.5|37.51| 37.5|   100.0|
    | 10|1546300801000| 37.5|37.51|37.51|   100.0|
    | 10|1546300802000|37.51|37.52|37.51|   100.0|
    | 20|1546300804000| null| null|12.67|   300.0|
    | 10|1546300806000| 37.5|37.51|37.51|   100.0|
    | 10|1546300807000| 37.5|37.51| 37.5|   200.0|
    +---+-------------+-----+-----+-----+--------+

    :param trades: DataFrame of trade events
    :param prices: DataFrame of price events
    :return: A DataFrame of the combined events and filled.
    """

    in_prices=prices.withColumn('prices_id',expr('id')).withColumn('prices_timestamp',expr('timestamp'))
    in_prices=in_prices.drop('id','timestamp')
    
    # create a window partitioned by id and ordered by timestamp
    windowSpec_price = Window.partitionBy("prices_id").orderBy("prices_timestamp")
    windowSpec_trade = Window.partitionBy("id").orderBy("timestamp")

    # create a new column next_timestamp for prices
    in_prices = in_prices.withColumn("next_timestamp", F.lead("prices_timestamp", default=None).over(windowSpec_price))
    
    # create a new column next_trade_timestamp for trades
    in_trades = trades.withColumn("next_trade_timestamp", F.lead("timestamp", default=None).over(windowSpec_trade))

    # select the desired columns for prices
    in_prices = in_prices.select("prices_id", "prices_timestamp", "bid", "ask", "next_timestamp")
    
    # Create dataframe for Trade with trade timestamp values in between Price timestamps
    df_trade = in_trades.join(in_prices,(in_trades.id == in_prices.prices_id) & (in_trades.timestamp >= in_prices.prices_timestamp) \
                     & ((in_trades.timestamp < in_prices.next_timestamp)|(in_prices.next_timestamp.isNull())),'left')
    
    df_trade=df_trade.select(expr('id'),\
                      expr('timestamp'),\
                      expr('bid'),\
                      expr('ask'),\
                      expr('price'),\
                      expr('quantity'),\
                     )
    # Create dataframe for Price with trade timestamp values in between Price timestamps if not then Null
    
    df_price = in_trades.join(in_prices,(in_trades.id == in_prices.prices_id) & (in_trades.timestamp <= in_prices.prices_timestamp)\
                      & ((in_trades.next_trade_timestamp > in_prices.prices_timestamp)|(in_trades.next_trade_timestamp.isNull())),'right')
    
    df_price=df_price.select(expr('prices_id').alias('id'),\
                      expr('prices_timestamp').alias('timestamp'),\
                      expr('bid'),\
                      expr('ask'),\
                      expr('price'),\
                      expr('quantity'),\
                     )
    
    #Concatenate the dataframes trades and prices
    result=df_price.unionAll(df_trade).orderBy("timestamp")
    return result
