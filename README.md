# Streamlit - Snowflake Ingest Observer

This is a Streamlit (in Snowflake) app that provides basic ***live*** data ingestion stats.


Here's a preview: 

<p align="center">

<img src="https://github.com/sfc-gh-vshiv/snowflake-ingest-observer/blob/main/snowflake-ingest-observer.gif">

</p>

***

Pre-requisites:

- Any table in Snowflake that data is being ingested into - batch or streaming. Good examples are in this Snowflake Quickstart: [Tour of Ingest](https://quickstarts.snowflake.com/guide/tour_of_ingest/index.html?index=..%2F..index#0)


Steps to implement:

1. Copy + paste the code from `sis-app.py` to a new Streamlit app in your Snowflake account (on Snowsight)

2. Run the app

3. Within the app:

    - Choose the table of choice to observe
    - Turn on auto-refresh at the bottom of the app and choose a refresh interval of choice. Best choice is to keep it same as the parameter `buffer.flush.time` in your kafka configuration
    


***