########################################################
##############  Import python packages   ###############
########################################################
import streamlit as st
from snowflake.snowpark.context import get_active_session
from datetime import datetime
import time
import pandas as pd
from snowflake.snowpark.functions import col
import altair as alt

########################################################
######      Page config and Snowflake session      #####
########################################################
# Title & Subheader

st.set_page_config(layout="centered")
st.title("Snowflake Ingest Observer :snowflake: ")
st.caption("This app provides ***live*** ingest stats for tables in this app's schema.\
            Run one instance of the app per table.")
st.markdown("***")

# Get Snowflake session
session = get_active_session()

########################################################
#################      Functions       #################
########################################################

#### For human readable byte size ####
def human_readable_size(bytes):
   """
   Converts a byte size to a human-readable format with units.
   Division is by 1000 and 1024 to comply with Altair charts

   Args:
     bytes: The size in bytes.

   Returns:
     A string representing the size in a human-readable format.
   """
   if bytes < 1000:
       return f"{round(bytes, 2)} B"
   elif bytes < 1000000:
       return f"{round(bytes / 1000, 2)} KB"
   elif bytes < 1000000000:
       return f"{round(bytes / 1000 / 1000, 2)} MB"
   elif bytes < 1000000000000:
       return f"{round(bytes / 1000 / 1000 / 1000, 2)} GB"
   else:
       return f"{round(bytes / 1000 / 1000 / 1000 / 1000, 2)} TB"


########################################################
###############      Data retrieval      ###############
########################################################

schema = session.get_current_schema().replace('"',"'")

st.write(f"Choose a table to observe, from the current app schema {schema}:")

# Get initial set of tables and display
data_sql = f'''
SELECT 
    TABLE_NAME,
    ROW_COUNT,
    BYTES,
    TO_CHAR(CREATED, 'YYYY-MM-DD HH24:MI TZH:TZM') AS CREATED_TS,
    LAST_ALTERED AS TABLE_LAST_ALTERED
FROM INFORMATION_SCHEMA.TABLES 
WHERE 1=1
AND TABLE_TYPE = 'BASE TABLE'
AND TABLE_SCHEMA = {schema}
ORDER BY TABLE_LAST_ALTERED DESC;'''

data_df = session.sql(data_sql).collect()

# Dropdown
tab_cols = st.columns (2)

with tab_cols[0]:
    tab_name = st.selectbox(label="Select a table"
                            , options=data_df
                           ,label_visibility='hidden')


# Filter the data for the selected table
# Streamlit doesn't refresh dataframe filters after selectbox selection
# Hence, need to rern the query after the selection

tables_sql = f'''
SELECT 
    TABLE_NAME,
    ROW_COUNT,
    BYTES AS TABLE_SIZE,
    TO_CHAR(CREATED, 'YYYY-MM-DD HH24:MI TZH:TZM') AS TABLE_CREATED,
    LAST_ALTERED AS TABLE_LAST_ALTERED
FROM INFORMATION_SCHEMA.TABLES 
WHERE 1=1
AND TABLE_SCHEMA = {schema}
AND TABLE_NAME = '{tab_name}';'''


record_stats_df = session.sql(tables_sql).collect()[0]
#st.table(record_stats_df)
        

########################################################
###############      Delta Tracking      ###############
########################################################

# Number of times the app is refreshed
if "times_refreshed" not in st.session_state:
    st.session_state["times_refreshed"] = 0

# Table name
if "table_name" not in st.session_state:
    st.session_state["table_name"] = [record_stats_df[0]]
else:
    st.session_state["table_name"].append(record_stats_df[0])
    
#### Record Counts ####

# Current Count
if "current_count" not in st.session_state:
    st.session_state["current_count"] = [int(record_stats_df[1])]
else:
    st.session_state["current_count"].append(int(record_stats_df[1]))
current_count = record_stats_df[1]


#### Table Size ####
# Current Size
current_size = record_stats_df[2]

if "current_size" not in st.session_state:
    st.session_state["current_size"] = [current_size]
else:
    st.session_state["current_size"].append(current_size)


#### Timestamps ####

# Record create timestamp
rec_create_timestamp = record_stats_df[4]

if "rec_create_timestamp" not in st.session_state:
    st.session_state["rec_create_timestamp"] = [rec_create_timestamp]
else:
    st.session_state["rec_create_timestamp"].append(rec_create_timestamp)


#### DFs for stats ####

# Get initial data from session state
data=[st.session_state["table_name"]
      , st.session_state["rec_create_timestamp"]
      ,st.session_state["current_count"]
      ,st.session_state["current_size"]]
stats_df = pd.DataFrame(data).T
stats_df.columns=['table_name', 'rec_create_timestamp','current_count', 'current_size']
stats_df = stats_df[stats_df.table_name == tab_name]

# Time delta
stats_df['time_delta_s'] = stats_df['rec_create_timestamp']\
                                .diff().dt.total_seconds().fillna(0).astype(int)

# Rowcount delta
stats_df['rowcount_delta'] = stats_df['current_count']\
                                .diff().fillna(0).astype(int)

# Tableize delta
stats_df['tabsize_delta'] = stats_df['current_size']\
                                .diff().fillna(0).astype(float)

# Throughput
stats_df['rows_per_sec'] = (stats_df['rowcount_delta']/stats_df['time_delta_s'])\
                             .round(decimals=0).fillna(0).astype(int)

# st.table(stats_df)


########################################################
############       App Display Logic     ##############
########################################################

st.markdown("***")

### Table name and created time ###
tab_cols = st.columns((1, 4, 1))
with tab_cols[1]:
    st.markdown(f"###  **{tab_name}**")

tab_cols = st.columns((0.5, 0.7, 2, 1))
with tab_cols[2]:
    st.caption(f"Table created: \
                {record_stats_df[3]}")



### Metrics ###
stat_section = st.columns((1.2, 1, 0.7))

# Last Record Insert Time
with stat_section[0]:
    st.metric(label="Last Record Insert Time"
              ,value=rec_create_timestamp.strftime("%H:%M:%S %z")
             , delta=rec_create_timestamp.strftime("%m/%d/%Y")
             , delta_color="off"
             )
    
# Row count
with stat_section[1]:
    count_delta = stats_df['rowcount_delta'].iat[-1]
    st.metric(label="Record Count"
              , value= f"{current_count:,}"
              , delta=f"{count_delta:,}"
              )

# Approximate table size
with stat_section[2]:
    size_delta = stats_df['tabsize_delta'].iat[-1]
    st.metric(label="Table Size"
              , value= f"{human_readable_size(current_size)}"
              , delta=f"{human_readable_size(size_delta)}"
              , help="Representative purposes only. Might not match your actual storage billed.")


### Altair Line Charts ###
st.markdown("***")

# Throughput

througput = (
        alt.Chart(stats_df[(stats_df.table_name == tab_name) & (stats_df.rows_per_sec >0)]
                  , title="Throughput", )
    .encode(
            x=alt.X("rec_create_timestamp"
                    , title="Timestamp"
                    , axis=alt.Axis(format=="%H:%M:%S")),
            y=alt.Y("rows_per_sec:Q"
                    , title="Rows/Sec"
                    ,axis=alt.Axis(format=",s")),
            tooltip=[alt.Tooltip("rec_create_timestamp"
                                 , title="Time"
                                 , format="%H:%M:%S"),
                    alt.Tooltip("rows_per_sec"
                                , title="Rows/Sec"
                                , format=",.3s")],
            color=alt.value("red")))

st.altair_chart(througput.mark_line() + througput.mark_point() 
                , use_container_width=True)


charts_section = st.columns((1, 1))

# Row counts
with charts_section[0]:
    rowcounts = (
        alt.Chart(stats_df[stats_df.table_name == tab_name], title="Row Counts")
        .encode(
            x=alt.X("rec_create_timestamp"
                    , title="Timestamp"
                    , axis=alt.Axis(format=="%H:%M:%S", grid=True)),
            y=alt.Y("current_count:Q"
                    , title="Row Count"
                    , axis=alt.Axis(format=",s", labelPadding=0)
                    , scale=alt.Scale(type='log', nice=False)),
            tooltip=[alt.Tooltip("rec_create_timestamp"
                                 , title="Time"
                                 , format="%H:%M:%S"),
                    alt.Tooltip("current_count"
                                , title="Rows Count"
                                , format=",.4s")],
            color=alt.value("green")))
    
    st.altair_chart(rowcounts.mark_area(opacity=0.5) + rowcounts.mark_circle())

# Table size
with charts_section[1]:
    tabsize = (
        alt.Chart(stats_df[stats_df.table_name == tab_name], title="Table Size")
        .encode(
            x=alt.X("rec_create_timestamp"
                    , title="Timestamp"
                    , axis=alt.Axis(format=="%H:%M:%S", grid=True)
                    , scale=alt.Scale(type='time', nice=True)),
            y=alt.Y("current_size:Q"
                    , title="Table Size"
                    , axis=alt.Axis(format=",s", grid=True)
                    , scale=alt.Scale(type='log', nice=False)),
            tooltip=[alt.Tooltip("rec_create_timestamp"
                                 , title="Time"
                                 , format="%H:%M:%S"),
                    alt.Tooltip("current_size"
                                , title="Table Size"
                                , format=",.4s")],
        color=alt.value("navy")))
    
    st.altair_chart(tabsize.mark_area(opacity=0.5, line={'color':'darkgreen'}) + tabsize.mark_circle())
    
    


# # Display the SQL Query
# st.markdown("***")
# st.markdown("#### SQL Query")
# st.code(count_sql, language='SQL')
# st.markdown("***")


### Refresh section ###
st.markdown("***")
refresh_section = st.columns((1, 1))

st.session_state["times_refreshed"] += 1
with refresh_section[0]:
    refresh_mode = st.radio(
        "Refresh Automatically?", options=["Yes", "No"], index=1, horizontal=False)
    if refresh_mode == "Yes":
        interval = st.slider("Interval to refresh", min_value=5, max_value=60, value=10, step=2)

with refresh_section[1]:
    st.info(f"**Times Refreshed** :\
        {st.session_state['times_refreshed']} ")
    

    if refresh_mode == "Yes":
        ## Spinner
        
        with st.spinner(f"Refreshing every {interval} secs.."):
            time.sleep(interval)
            st.experimental_rerun()
            
        
    if refresh_mode == "No":
        refresh = st.button("Refresh")
        if refresh:
            st.experimental_rerun()
            

st.markdown("***")

