import polars as pl
import streamlit as st
import altair as alt



# Set up the Streamlit page configuration
st.set_page_config(
    page_title= "Polars Stock Market Aggregations",
    layout="wide",
    initial_sidebar_state="expanded"   
)

#Define file paths for the aggregated data
FILE_AGG1 = "agg1_daily_avg_close_price.parquet"
FILE_AGG2 = "agg2_avg_volume_by_sector.parquet"
FILE_AGG3 = "agg3_simple_daily_return.parquet"

# Data Loading

@st.cache_data
def load_data(file_path):
    """Loads a Parquet file using Polars and caches the results in Streamlit."""
    try:
        df = pl.read_parquet(file_path)
        #Convert Polars Dataframe to Pandas dataframe for Altair compatibility
        return df.to_pandas()
    
    except pl.exceptions.ComputeError:
        #User-facing error message in English
        st.error(f"File not found: {file_path}")
        st.stop()
    except Exception as e:
        # User-facing error message in English
        st.error(f"An error occurred while loading data: {e}")
        st.stop()
        
# Load all aggregated data files + cleaned raw data
RAW_FILE = "cleaned.parquet"
raw_df = load_data(RAW_FILE)


#Load all aggregated data files
agg1_df = load_data(FILE_AGG1)
agg2_df = load_data(FILE_AGG2)
agg3_df = load_data(FILE_AGG3)

# Application title and introduction
st.title("Stock Market Data Analysis Dashboard")
st.markdown("This dashboard is built using data cleaned and aggregated by Polars ('cleaned.parquet'and 'aggX_*.parquet').")

#Sidebar Filters

with st.sidebar:
    # Header translated to English
    st.header("Filter Setting")
    
    #1. Sector Filter
    # Get unique sectors, convert to list and sort
    sectors = sorted(agg2_df['sector'].dropna().unique().tolist()) #dropna = exclude null
    selected_sector = st.multiselect(
        "Select Sector(s)",
        options = sectors,
        default= sectors[:3] #Select the first 3 by default    
    )
 
 
 
 #######problem
 
 
# Filter the daily average data (agg1) by selected sectors
    if selected_sector:   
        tickers_in_sector = sorted(
            # NOTE: We rely on 'ticker' being present in agg1_df for this filter to work correctly.
             raw_df[raw_df['sector'].isin(selected_sector)]['ticker'].dropna().unique().tolist()
        )
    else:
        # If no sector is selected, allow all tickers from agg1_df to be potentially selected
        tickers_in_sector = sorted(agg1_df['ticker'].dropna().unique().tolist())
    
     # 2. Ticker Filter (based on selected sectors)
    selected_tickers = st.multiselect(
        "Select Ticker(s) (Depends on Sector)",
        options=tickers_in_sector,
        default=tickers_in_sector[:5] # Select the first 5 by default
    )




#Visualization Layout
#1. Row: daily close price trend
st.header("1. daily closing price trend analysis")
st.markdown("Shows the trend of daily average closing prices for selected tickers.")

if selected_tickers:
    #Filter agg1 data based on final selected tickers
    trend_data = agg1_df[agg1_df['ticker'].isin(selected_tickers)]
    
    #Altair Chart (Line chart for Trend) 
    chart1 = alt.Chart(trend_data).mark_line().encode(
        x=alt.X('trade_date:T', title='Trade Date'), # T for Temporal (Date/Time)
        y=alt.Y('daily_avg_close_price:Q', title='Daily Average Close Price'), # :Q for Quantitative
        color='ticker:N', # :N for Nominal (Category)
        tooltip=['trade_date', 'ticker', 'daily_avg_close_price']     
    ).properties(
        title="Daily Average Close Price Trend by Ticker"
    ).interactive() # Enable zooming and panning
    
    st.altair_chart(chart1, use_container_width=True)
else:
    st.info("Please select at least one Ticker in the sidebar to display the trend.")

#2. Row: Average volume and daily return
col1, col2 = st.columns(2)

with col1:
    st.header("2.Average volume by Sector")
    st.markdown("Compares the average trading volume across different sectors.")
    
    # Filter agg2 data by selected sectors for visualization
    volume_data = agg2_df[agg2_df['sector'].isin(selected_sector)]
    
    if not volume_data.empty:
        # Altair Chart (Bar Chart for Volume)
        chart2 = alt.Chart(volume_data).mark_bar().encode(
            x=alt.X('avg_volume:Q', title='Average Volume', axis=alt.Axis(format='~s')),
            y=alt.Y('sector:N', title='Sector', sort='x', axis=alt.Axis(labelLimit=100)),
            tooltip=['sector', 'avg_volume']
        ).properties(
            title="Average Volume by Sector"
        )
        st.altair_chart(chart2, use_container_width=True)
        
    else:
        st.info("No volume data to display. Please select a sector in the sidebar.")

with col2:
    st.header("3. Daily Return Distribution")
    st.markdown("Shows the distribution (histogram) of daily returns for selected tickers.")

    if selected_tickers:
        # Filter agg3 data based on final selected tickers
        return_data = agg3_df[agg3_df['ticker'].isin(selected_tickers)]
        
        # Altair Chart (Histogram for Daily Return)
        chart3 = alt.Chart(return_data).mark_bar().encode(
            x=alt.X('simple_daily_return:Q', bin=True, title='Daily Return'),
            y=alt.Y('count()', title='Frequency'),
            color=alt.Color('ticker:N', title='Ticker'),
            tooltip=['ticker', 'count()']
        ).properties(
           
            title="Daily Return Histogram"
        ).interactive()

        st.altair_chart(chart3, use_container_width=True)
    else:
    
        st.info("Please select a Ticker in the sidebar to display the return distribution.")
        
   
   # Footer (Raw Data Display)

st.markdown("---")

if st.checkbox("Show Aggregated Data (agg1 - Daily Close)"):
    st.subheader("Daily Average Close Price Data")
    st.dataframe(agg1_df.head(100), use_container_width=True)

if st.checkbox("Show Aggregated Data (agg2 - Volume by Sector)"):
    st.subheader("Average Volume by Sector Data")
    st.dataframe(agg2_df.head(100), use_container_width=True)


if st.checkbox("Show Aggregated Data (agg3 - Daily Return)"):
    st.subheader("Daily Return Data")
    # Display columns relevant for the return data
    st.dataframe(agg3_df[['ticker', 'trade_date', 'simple_daily_return']].head(100), use_container_width=True)     

