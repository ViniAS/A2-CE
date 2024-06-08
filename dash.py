import streamlit as st
import pandas as pd
import plotly.express as px
from st_aggrid import AgGrid, GridOptionsBuilder

def load_data():
    """
    Carrega os dados necessários para o dashboard.
    """
    df_orders = pd.read_csv('data/Products_Purchased_Per_Minute.csv')
    df_revenue = pd.read_csv('data/Revenue_Per_Minute.csv')
    df_users = pd.read_csv('data/Unique_User_Views_Per_Product_Per_Minute.csv')
    df_avg_products = pd.read_csv('data/Average_Products_Per_Minute.csv')
    df_avg_revenue = pd.read_csv('data/Average_Revenue_Per_Minute.csv')
    df_avg_users = pd.read_csv('data/Average_Unique_Users_Per_Product_Per_Minute.csv')
    df_ranking = pd.read_csv('data/Most_Viewed_Products_Per_Minute_Last_Hour.csv')
    return df_orders, df_revenue, df_users, df_avg_products, df_avg_revenue, df_avg_users, df_ranking

def configure_page():
    """
    Configura o layout e o título da página.
    """
    st.set_page_config(layout="wide")
    st.title('Dashboard de Métricas de Vendas e Visualizações')

def create_metric_card(title, value):
    """
    Cria um card de métrica com título e valor, utilizando HTML para estilização.
    """
    st.markdown(f"""
        <div style="background-color: #f4f1ff; border-radius: 10px; padding: 10px; text-align: center;">
            <h3 style="color: #5f4b8b; margin-bottom: 0;">{title}</h3>
            <h2 style="color: #c084fc; margin-top: 5px;">{value}</h2>
        </div>
        """, unsafe_allow_html=True)

def plot_metric(column, data, x_axis, y_axis, title, avg_value):
    """
    Plota um gráfico de linha para a métrica especificada.
    """
    fig = px.line(data, x=x_axis, y=y_axis, title=title, color_discrete_sequence=['#5f4b8b'])
    fig.update_xaxes(rangeslider_visible=True)
    fig.add_hline(y=avg_value, line_color="#c084fc")
    column.plotly_chart(fig)

def display_ranking(df_ranking):
    """
    Exibe o ranking dos produtos mais visualizados na última hora com estilo aprimorado.
    """
    st.header('Ranking de Produtos Mais Visualizados na Última Hora')

    # Configurando AgGrid
    gb = GridOptionsBuilder.from_dataframe(df_ranking)
    gb.configure_pagination(paginationAutoPageSize=True)  # Adiciona paginação
    gb.configure_grid_options(domLayout='normal')
    grid_options = gb.build()

    AgGrid(
        df_ranking,
        gridOptions=grid_options,
        enable_enterprise_modules=False,
        fit_columns_on_grid_load=True,
        height=400,
        theme='material',  # Usa um tema moderno
    )

def main():
    """
    Função principal que organiza o fluxo do dashboard.
    """
    df_orders, df_revenue, df_users, df_avg_products, df_avg_revenue, df_avg_users, df_ranking = load_data()
    configure_page()
    col1, col2, col3 = st.columns(3)

    # Produtos comprados por minuto
    with col1:
        create_metric_card('Média de Produtos Comprados por Minuto', f"{df_avg_products['Average Products Per Minute'].iloc[0]:.2f}")
        plot_metric(col1, df_orders, 'PURCHASE DATE', 'QUANTITY', "Produtos Comprados por Minuto", df_avg_products['Average Products Per Minute'].iloc[0])

    # Faturamento por minuto
    with col2:
        create_metric_card('Média de Faturamento por Minuto', f"R$ {df_avg_revenue['Average Revenue Per Minute'].iloc[0]:.2f}")
        plot_metric(col2, df_revenue, 'REVENUE DATE', 'REVENUE', "Faturamento por Minuto", df_avg_revenue['Average Revenue Per Minute'].iloc[0])

    # Usuários únicos por produto por minuto
    with col3:
        create_metric_card('Média de Usuários Únicos por Minuto', f"{df_avg_users['Average Unique Users Per Product Per Minute'].iloc[0]:.2f}")
        plot_metric(col3, df_users, 'VIEW DATE', 'UNIQUE USERS', "Usuários Únicos por Produto por Minuto", df_avg_users['Average Unique Users Per Product Per Minute'].iloc[0])

    # Exibir ranking de produtos mais visualizados na última hora
    display_ranking(df_ranking)

if __name__ == "__main__":
    main()
