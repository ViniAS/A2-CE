from pyspark.sql import SparkSession
import streamlit as st
import plotly.express as px
from pyspark.sql import functions as F
from st_aggrid import AgGrid, GridOptionsBuilder
import pandas as pd
import time
from src.answer_q1 import answer_q1
from src.answer_q2 import answer_q2
from src.answer_q3 import answer_q3
from src.answer_q4 import answer_q4
from src.answer_q5 import answer_q5
from src.answer_q6 import answer_q6
from src.monitor_precos import monitor

# Inicia a sessão Spark
# spark = SparkSession.builder.appName("Dashboard").getOrCreate()
jdbc_driver_path = "jdbc/postgresql-42.7.3.jar"

# Cria uma sessão Spark
spark = SparkSession.builder \
    .appName("Dash") \
    .config("spark.jars", jdbc_driver_path) \
    .getOrCreate()

loading_times = {}

def fetch_data(store=None):
    """
    Fetches necessary data for the dashboard using Spark.
    """
    global loading_times
    loading_times.clear()   
    start_time = time.time()
    df_orders = answer_q1(spark, store, table=True)
    loading_times['q1_table'] = time.time() - start_time

    start_time = time.time()
    df_avg_products = answer_q1(spark, store, table=False)
    loading_times['q1_number'] = time.time() - start_time
    
    start_time = time.time()
    df_revenue = answer_q2(spark, store, table=True)

    loading_times['q2_table'] = time.time() - start_time

    start_time = time.time()
    df_avg_revenue = answer_q2(spark, store, table=False)
    loading_times['q2_number'] = time.time() - start_time

    start_time = time.time()
    df_users = answer_q3(spark, store, table=True)
    loading_times['q3_table'] = time.time() - start_time

    start_time = time.time()
    df_avg_users = answer_q3(spark, store, table=False)
    loading_times['q3_number'] = time.time() - start_time

    start_time = time.time()
    df_ranking = answer_q4(spark, store)
    df_ranking.show()
    loading_times['q4'] = time.time() - start_time

    start_time = time.time()
    df_median_views = answer_q5(spark, store)
    loading_times['q5'] = time.time() - start_time

    start_time = time.time()
    df_excess_sales = answer_q6(spark, store)
    loading_times['q6'] = time.time() - start_time

    # df_stores = spark.read.csv('data/data_mock/Stores.csv', header=True)

    return df_orders, df_revenue, df_users, df_avg_products, df_avg_revenue, df_avg_users, df_ranking, df_median_views, df_excess_sales, None

def create_metric_card(title, value):
    """
    Creates a metric card with the title above and the value below using HTML for styling.
    Ensures that the value doesn't overflow the box.
    """
    st.markdown(f"""
        <div style="background-color: #5f4b8b; border-radius: 10px; padding: 20px; text-align: center; height: 150px; margin-bottom: 20px;">
            <h3 style="color: white; margin-bottom: 5px; font-size: 16px;">{title}</h3>
            <h1 style="color: white; font-size: 30px; margin-top: 5px;">{value}</h1>
        </div>
        """, unsafe_allow_html=True)

def plot_metric(column, data, x_axis, y_axis, title, avg_value):
    """
    Plots a line chart for the specified metric.
    """
    data_list = data.select(x_axis, y_axis).collect()
    x_values = [row[x_axis] for row in data_list]
    y_values = [row[y_axis] for row in data_list]

    fig = px.line(x=x_values, y=y_values, title=title, labels={0: x_axis, 1: y_axis}, color_discrete_sequence=['#5f4b8b'])
    fig.update_xaxes(rangeslider_visible=True)
    fig.add_hline(y=avg_value, line_color="#c084fc")
    column.plotly_chart(fig)

def display_ranking(df_ranking):
    """
    Displays the ranking of the most viewed products in the last hour with enhanced styling.
    """
    st.header('Ranking of Most Viewed Products in the Last Hour')
    
    ranking_df = df_ranking.toPandas()  # Convert to Pandas DataFrame for display
    gb = GridOptionsBuilder.from_dataframe(ranking_df)
    gb.configure_pagination(paginationAutoPageSize=True)  # Adds pagination
    gb.configure_grid_options(domLayout='normal')
    grid_options = gb.build()

    AgGrid(
        ranking_df,
        gridOptions=grid_options,
        enable_enterprise_modules=False,
        fit_columns_on_grid_load=True,
        height=400,
        theme='alpine',  
    )

def display_text_metrics(df_median_views, df_excess_sales):
    """
    Displays the metrics for median views before purchase and products sold without stock.
    """
    st.header('Other Important Metrics')

    median_views_value = df_median_views
    excess_sales_value = df_excess_sales.collect()[0][0]

    create_metric_card(
        'Median Views Before Purchase',
        f"{median_views_value:.2f}"
    )

    create_metric_card(
        'Number of Products Sold Without Stock',
        f"{excess_sales_value:.0f}"
    )

def display_loading_times():
    """
    Displays the loading times stored in the global dictionary using an enhanced styling grid.
    """
    st.header("Data Loading Times")
    
    times_df = pd.DataFrame(list(loading_times.items()), columns=['DataFrame', 'Time (seconds)'])
    
    # Configurando a exibição com AgGrid
    gb = GridOptionsBuilder.from_dataframe(times_df)
    gb.configure_pagination(paginationAutoPageSize=True)  
    gb.configure_side_bar()  
    gb.configure_grid_options(domLayout='normal')
    grid_options = gb.build()

    AgGrid(
        times_df,
        gridOptions=grid_options,
        enable_enterprise_modules=False,
        fit_columns_on_grid_load=True,
        height=400,
        theme='alpine',  
    )

def display_price_monitor(prices):
    """
    Displays the products found by the price monitor with enhanced styling.
    """
    st.header('Products Found by the Price Monitor')
    
    prices_df = prices.toPandas()  # Convert to Pandas DataFrame for display
    gb = GridOptionsBuilder.from_dataframe(prices_df)
    gb.configure_pagination(paginationAutoPageSize=True)  # Adds pagination
    gb.configure_grid_options(domLayout='normal')
    grid_options = gb.build()

    AgGrid(
        prices_df,
        gridOptions=grid_options,
        enable_enterprise_modules=False,
        fit_columns_on_grid_load=True,
        height=400,
        theme='alpine',  
    ) 

def price_monitor_page():
    """
    Configures and displays the Price Monitor page.
    """
    st.title('Price Monitor')
    st.write('Use the options below to search for products based on your preferences.')
    
    col1, col2 = st.columns([1, 2])
    
    with col1:
        months_to_consider = st.number_input('Number of months to consider:', min_value=1, max_value=36, step=1, key="months_input")
        
    with col2:
        percentage_discount = st.slider('Percentage discount (%):', min_value=0, max_value=100, step=1, key="discount_slider")
    
    if st.button('Buscar produtos'):
        prices = monitor(spark, months_to_consider, percentage_discount)
        prices.show()
        display_price_monitor(prices)

        

def configure_dashboard_page():
    """
    Configura e exibe a página do Dashboard.
    """
    col1, col2 = st.columns([4, 1])  

    with col1:
        st.title('Sales and Views Metrics Dashboard')
    with col2:
        st.markdown("""
            <style>
                div.stButton > button:first-child {
                    font-size: 16px;
                    height: 50px;  
                    width: 100%;
                    border-radius: 5px;
                    border: 1px solid rgba(27, 31, 35, 0.15);
                    margin-top: -5px;  
                    background-color: #5f4b8b;  
                    color: white;  
                    transition: background-color 0.3s, box-shadow 0.3s;  
                }
                div.stButton > button:first-child:hover {
                    background-color: #4b3a6b; 
                    border: 1px solid rgba(27, 31, 35, 0.35);
                    color: white;
                }
                div.stButton > button:first-child:active {
                    background-color: #3a2d54;  
                    box-shadow: 0 2px 5px -1px rgba(0, 0, 0, 0.2); 
                    color: white;
                }
            </style>
            """, unsafe_allow_html=True)
        if st.button('Refresh Data'):
            st.experimental_rerun()
    
    # Access the shop_data table
    df_stores = spark.read.jdbc(url="jdbc:postgresql://localhost:5432/source_db", table="shop_data", properties={"user": "postgres", "password": "senha", "driver": "org.postgresql.Driver"})
    store_names = ["All"] + [row['shop_name'] for row in df_stores.collect()]

    col1, col2 = st.columns([1, 3])
    with col1:
        store = st.selectbox('Choose a store:', store_names)
        if store == "All":
            store = None
        else:
            store = df_stores[df_stores['shop_name'] == store].collect()[0]['shop_id']

    df_orders, df_revenue, df_users, df_avg_products, df_avg_revenue, df_avg_users, df_ranking, df_median_views, df_excess_sales, _ = fetch_data(store)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        avg_products_value = df_avg_products.collect()[0][0]
        create_metric_card('Average Products Purchased per Minute', f"{avg_products_value:.2f}")
        plot_metric(col1, df_orders, 'minute', 'quantity', "Products Purchased per Minute", avg_products_value)

    with col2:
        avg_revenue_value = df_avg_revenue.collect()[0][0]
        create_metric_card('Average Revenue per Minute', f"R$ {avg_revenue_value:.2f}")
        plot_metric(col2, df_revenue, 'minute', 'revenue_per_minute', "Revenue per Minute", avg_revenue_value)

    with col3:
        avg_users_value = df_avg_users.collect()[0][0]
        create_metric_card('Average Unique Users per Minute', f"{avg_users_value:.2f}")
        plot_metric(col3, df_users, 'minute', 'unique_users', "Unique Users per Product per Minute", avg_users_value)

    col_left, col_right = st.columns((2, 1))

    with col_left:
        display_ranking(df_ranking)

    with col_right:
        display_text_metrics(df_median_views, df_excess_sales)

    display_loading_times()

def main():
    """
    Function that organizes the dashboard flow.
    Defines a radio control in the sidebar to navigate between different pages of the dashboard.
    """
    st.set_page_config(layout="wide")
    
    menu = ["Dashboard", "Price Monitor"]
    choice = st.sidebar.selectbox("Menu", menu)
    
    if choice == "Dashboard":
        configure_dashboard_page()
    elif choice == "Price Monitor":
        price_monitor_page()

if __name__ == "__main__":
    main()
