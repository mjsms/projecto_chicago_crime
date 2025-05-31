import streamlit as st
import time

# Importing critical functions that deal with data stream (Spark/Kafka side)
from data_streaming import ( spark_initialize, data_stream_spark, 
                show_tables, show_status, get_table_dataframe )

# Caching the function that will access the running 
# Spark/Kafka data query (a DataFrame)
@st.cache_resource
def get_data():
    return get_table_dataframe(st.session_state.spark, st.session_state.table)

# Showing results of data stream processing, 
# as long as there is a SparkSession running
def results():

    if 'spark' not in st.session_state:
        return
    
    status_text = st.empty()
    progress_bar = st.progress(0)
    placeholder = st.empty()
    sleeptime = 0.8
    maxiterations = 30
    top_location = 20
    top_district = 20

    # Iterative update
    for i in range(maxiterations):
        time.sleep(sleeptime)
        # getting data at this point in time
        df = get_data()
        count = df.count()
        status_text.warning(f'Processing...  Iteration {i} with in-between delay of {sleeptime} second(s). Messages/records processed so far: {count}.')
        cols1 = ['Location_Description'] 
        df_location = df.groupBy(cols1).count().orderBy('count', ascending=False).limit(top_location).toPandas()
        cols2 = ['Description']
        df_description = df.groupBy(cols2).count().orderBy('count', ascending=False).limit(top_district).toPandas()


        with placeholder.container():

            # Each chart in one column, so two columns required
            fig_col1, fig_col2 = st.columns(2)
            with fig_col1:
                st.markdown('### Location')
                st.markdown(f'**Counting of crimes by location - Top {top_location}**')
                st.bar_chart(data=df_location, y='count', x=cols1[0], horizontal=True)
            with fig_col2:
                st.markdown('### Description')
                st.markdown(f'**Counting of crimes by description - Top {top_district}**')
                st.bar_chart(data=df_description, y='count', x=cols2[0], horizontal=True)

            # Show the related dataframes
            st.markdown('### Detailed tables view')
            st.markdown('**Location**')
            st.dataframe(df_location)
            st.markdown('**Description**')
            st.dataframe(df_description)
    
        progress_bar.progress(i)
  
    progress_bar.empty()
    status_text.success(f'Final results are shown after processing {count} messages/records.')

# Page to hold results
def page_results():
    st.empty()
    st.header(':one: Data stream processing')
    st.subheader('Results')
    results()
    
# Page to hold information about the app
def page_about():
    st.empty()
    st.header(':two: About')
    st.subheader('Lab class handout #6')
    st.write('Data streaming with Apache Spark and Apache Kafka')
    st.badge('Streamlit version', icon='ℹ️', color='blue')
    
# Entry point
def main():
    
    # Page config
    st.set_page_config(
        page_title = 'Crimes data streaming',
        initial_sidebar_state = 'expanded',
        layout = 'wide'
    )
    # App title
    st.title('Crimes data streaming')
    st.divider()
    with st.sidebar:
        st.empty()
        st.header('Algoritmos para Big Data')

    brokers = 'localhost:9092'
    topic = 'crime-predictions'
    table = 'chicagocrimetable'

    # As code is running everytime the user interacts with, 
    # we must make sure that the spark side only starts once

    if 'spark' not in st.session_state:
        spark = spark_initialize()
        query = data_stream_spark(spark, brokers, topic, table)
        st.session_state.spark = spark
        st.session_state.table = table
        # just to check in the terminal
        show_status(spark, query)
        show_tables(spark)
        
    pages = [ st.Page(page_results, title='Results'),
              st.Page(page_about, title='About'),
            ]
    pg = st.navigation(pages)
    pg.run()

# Execution
if __name__ == "__main__":
    main()
