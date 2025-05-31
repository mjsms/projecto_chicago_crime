import time

# Importing critical functions that deal with data stream
from data_streaming import ( spark_initialize, data_stream_spark, 
            show_status, show_tables, show_sink_table, get_table_dataframe )

brokers = 'localhost:9092'
topic = 'chicago-crime'
table = 'chicagocrimestable'

# Showing results of data stream processing
def show_spark_results(spark, table):
    df = get_table_dataframe(spark, table)
    # cols_interest = ['timestamp','Asin','Group','Format','Title','Author','Publisher']
    
    sleeptime = 0.8
    maxiterations = 30
    top_authors = 20
    top_publishers = 20

    # Iterative update
    for i in range(maxiterations):
        time.sleep(sleeptime)
        print(f'Processing...  Iteration {i} with in-between delay of {sleeptime} second(s)')
        print(f'Number of records processed so far: {df.count()}.')

        #df.select(cols_interest).show(truncate=False)
        #df.show(5, truncate=False)
        #show_sink_table(spark, table)

        print('Aggregated information as it stands (top 20):')
        df.groupBy('Location_Description').count().orderBy('count', ascending=False).limit(top_authors).show(truncate=False)
        df.groupBy('District').count().orderBy('count', ascending=False).limit(top_publishers).show(truncate=False)
        #df.groupBy('Group', 'Format').count().show(truncate=False)
        

# Execution

spark = spark_initialize()
query = data_stream_spark(spark, brokers, topic, table)

show_status(spark, query)
show_tables(spark)
show_sink_table(spark, table)
show_spark_results(spark, table)
