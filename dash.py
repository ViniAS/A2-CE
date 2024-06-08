"""
Nomes das Colunas e Descrições para Cada Base de Dados que usei de teste:
obs: depois tem que adptar para os dados reais usando a query do banco de dados, mas acredito que se conseguir fazer a query com essas colunas, vai funcionar de boa

1. Products_Purchased_Per_Minute.csv
    - Colunas:
        - PURCHASE DATE: O timestamp quando a compra foi realizada.
        - QUANTITY: O número de produtos comprados.
    - Descrição: Esta base de dados contém o número de produtos comprados por minuto juntamente com o timestamp.

2. Revenue_Per_Minute.csv
    - Colunas:
        - REVENUE DATE: O timestamp quando a receita foi registrada.
        - REVENUE: O valor da receita gerada.
    - Descrição: Esta base de dados contém a receita gerada por minuto juntamente com o timestamp.

3. Unique_User_Views_Per_Product_Per_Minute.csv
    - Colunas:
        - VIEW DATE: O timestamp quando o produto foi visualizado.
        - PRODUCT ID: O ID do produto visualizado.
        - UNIQUE USERS: O número de usuários únicos que visualizaram o produto.
    - Descrição: Esta base de dados contém o número de usuários únicos visualizando cada produto por minuto juntamente com o timestamp e ID do produto

4. Average_Products_Per_Minute.csv
    - Colunas:
        - Average Products Per Minute: O número médio de produtos comprados por minuto.
    - Descrição: Esta base de dados contém o número médio de produtos comprados por minuto.

5. Average_Revenue_Per_Minute.csv
    - Colunas:
        - Average Revenue Per Minute: A receita média gerada por minuto.
    - Descrição: Esta base de dados contém a receita média gerada por minuto.

6. Average_Unique_Users_Per_Product_Per_Minute.csv
    - Colunas:
        - Average Unique Users Per Product Per Minute: O número médio de usuários únicos visualizando cada produto por minuto.
    - Descrição: Esta base de dados contém o número médio de usuários únicos visualizando cada produto por minuto.

7. Most_Viewed_Products_Per_Minute_Last_Hour.csv
    - Colunas:
        - VIEW DATE: O timestamp quando o produto foi visualizado.
        - PRODUCT ID: O ID do produto visualizado.
        - VIEW COUNT: O número de visualizações que o produto recebeu.
        - STORE ID: O ID da loja onde o produto foi visualizado.
    - Descrição: Esta base de dados contém os produtos mais visualizados na última hora juntamente com o timestamp, ID do produto, número de visualizações e ID da loja.

8. Median_Views_Before_Purchase.csv
    - Colunas:
        - MEDIAN_VIEWS: O número mediano de vezes que um produto foi visualizado antes da compra.
    - Descrição: Esta base de dados contém o número mediano de visualizações antes de um produto ser comprado.

9. Total_Excess_Sales.csv
    - Colunas:
        - Total Excess Sales: O número de produtos vendidos sem disponibilidade de estoque.
    - Descrição: Esta base de dados contém o número total de produtos vendidos sem disponibilidade de estoque.

10. Stores.csv
    - Colunas:
        - Store ID: O ID da loja.
        - Store Name: O nome da loja.
    - Descrição: Esta base de dados contém as informações das lojas, incluindo o ID e o nome da loja.
"""


import streamlit as st
import pandas as pd
import plotly.express as px
from st_aggrid import AgGrid, GridOptionsBuilder

def fetch_data(store=None):
    """
    Simulates fetching necessary data for the dashboard.
    In the future, this will be replaced by database queries.
    """
    # Simulating data reading
    df_orders = pd.read_csv('data/Products_Purchased_Per_Minute.csv')
    df_revenue = pd.read_csv('data/Revenue_Per_Minute.csv')
    df_users = pd.read_csv('data/Unique_User_Views_Per_Product_Per_Minute.csv')
    df_avg_products = pd.read_csv('data/Average_Products_Per_Minute.csv')
    df_avg_revenue = pd.read_csv('data/Average_Revenue_Per_Minute.csv')
    df_avg_users = pd.read_csv('data/Average_Unique_Users_Per_Product_Per_Minute.csv')
    df_ranking = pd.read_csv('data/Most_Viewed_Products_Last_Hour.csv')
    df_median_views = pd.read_csv('data/Median_Views_Before_Purchase.csv')
    df_excess_sales = pd.read_csv('data/Total_Excess_Sales.csv')
    df_stores = pd.read_csv('data/Stores.csv')

    # Just to see if it filters:
    if store != 'All':
        store_id = df_stores[df_stores['Store Name'] == store]['Store ID'].iloc[0]
        df_ranking = df_ranking[df_ranking['STORE ID'] == store_id]

    return df_orders, df_revenue, df_users, df_avg_products, df_avg_revenue, df_avg_users, df_ranking, df_median_views, df_excess_sales, df_stores

def configure_page():
    """
    Configures the layout and title of the page.
    """
    st.set_page_config(layout="wide")
    st.title('Sales and Views Metrics Dashboard')

def create_metric_card(title, value):
    """
    Creates a metric card with a title and value using HTML for styling.
    """
    st.markdown(f"""
        <div style="background-color: #f4f1ff; border-radius: 10px; padding: 20px; text-align: center; height: 150px; margin-bottom: 20px;">
            <h3 style="color: #5f4b8b; margin-bottom: 10px;">{title}</h3>
            <h2 style="color: #c084fc; margin-top: 0; font-size: 24px;">{value}</h2>
        </div>
        """, unsafe_allow_html=True)

def plot_metric(column, data, x_axis, y_axis, title, avg_value):
    """
    Plots a line chart for the specified metric.
    """
    fig = px.line(data, x=x_axis, y=y_axis, title=title, color_discrete_sequence=['#5f4b8b'])
    fig.update_xaxes(rangeslider_visible=True)
    fig.add_hline(y=avg_value, line_color="#c084fc")
    column.plotly_chart(fig)

def display_ranking(df_ranking):
    """
    Displays the ranking of the most viewed products in the last hour with enhanced styling.
    """
    st.header('Ranking of Most Viewed Products in the Last Hour')

    # Configuring AgGrid
    gb = GridOptionsBuilder.from_dataframe(df_ranking)
    gb.configure_pagination(paginationAutoPageSize=True)  # Adds pagination
    gb.configure_grid_options(domLayout='normal')
    grid_options = gb.build()

    AgGrid(
        df_ranking,
        gridOptions=grid_options,
        enable_enterprise_modules=False,
        fit_columns_on_grid_load=True,
        height=400,
        theme='material',  # Uses a modern theme
    )

def display_text_metrics(df_median_views, df_excess_sales):
    """
    Displays the metrics for median views before purchase and products sold without stock.
    """
    st.header('Other Important Metrics')

    create_metric_card(
        'Median Views Before Purchase',
        f"{df_median_views['MEDIAN_VIEWS'].iloc[0]:.2f}"
    )

    create_metric_card(
        'Number of Products Sold Without Stock',
        f"{df_excess_sales['Total Excess Sales'].iloc[0]:.0f}"
    )

def main():
    """
    Main function that organizes the dashboard flow.
    """
    configure_page()
    
    # Load store data
    df_stores = pd.read_csv('data/Stores.csv')
    store_names = ["All"] + df_stores['Store Name'].tolist()

    col1, col2 = st.columns([1, 3])
    with col1:
        store = st.selectbox('Choose a store:', store_names)

    # Load data based on selected store
    df_orders, df_revenue, df_users, df_avg_products, df_avg_revenue, df_avg_users, df_ranking, df_median_views, df_excess_sales, _ = fetch_data(store)
    
    col1, col2, col3 = st.columns(3)
    
    # Products purchased per minute
    with col1:
        create_metric_card('Average Products Per Minute', f"{df_avg_products['Average Products Per Minute'].iloc[0]:.2f}")
        plot_metric(col1, df_orders, 'PURCHASE DATE', 'QUANTITY', "Products Purchased per Minute", df_avg_products['Average Products Per Minute'].iloc[0])

    # Revenue per minute
    with col2:
        create_metric_card('Average Revenue per Minute', f"R$ {df_avg_revenue['Average Revenue Per Minute'].iloc[0]:.2f}")
        plot_metric(col2, df_revenue, 'REVENUE DATE', 'REVENUE', "Revenue per Minute", df_avg_revenue['Average Revenue Per Minute'].iloc[0])

    # Unique users per product per minute
    with col3:
        create_metric_card('Average Unique Users per Minute', f"{df_avg_users['Average Unique Users Per Product Per Minute'].iloc[0]:.2f}")
        plot_metric(col3, df_users, 'VIEW DATE', 'UNIQUE USERS', "Unique Users per Product per Minute", df_avg_users['Average Unique Users Per Product Per Minute'].iloc[0])

    col_left, col_right = st.columns((2, 1))

    # Display ranking of most viewed products in the last hour on the left
    with col_left:
        display_ranking(df_ranking)

    # Display other important metrics on the right
    with col_right:
        display_text_metrics(df_median_views, df_excess_sales)

if __name__ == "__main__":
    main()
