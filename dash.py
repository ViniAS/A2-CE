import streamlit as st
import pandas as pd
import plotly.express as px
from st_aggrid import AgGrid, GridOptionsBuilder

def fetch_data(store=None):
    """
    Simula a busca de dados necessários para o dashboard.
    No futuro, isso será substituído por consultas ao banco de dados.
    """


    # Simulação de leitura dos dados
    df_orders = pd.read_csv('data/Products_Purchased_Per_Minute.csv')
    df_revenue = pd.read_csv('data/Revenue_Per_Minute.csv')
    df_users = pd.read_csv('data/Unique_User_Views_Per_Product_Per_Minute.csv')
    df_avg_products = pd.read_csv('data/Average_Products_Per_Minute.csv')
    df_avg_revenue = pd.read_csv('data/Average_Revenue_Per_Minute.csv')
    df_avg_users = pd.read_csv('data/Average_Unique_Users_Per_Product_Per_Minute.csv')
    df_ranking = pd.read_csv('data/Most_Viewed_Products_Per_Minute_Last_Hour.csv')
    df_median_views = pd.read_csv('data/Median_Views_Before_Purchase.csv')
    df_excess_sales = pd.read_csv('data/Total_Excess_Sales.csv')
    df_stores = pd.read_csv('data/Stores.csv')

    # Só para ver se filtra:
    if store != 'Todas':
        store_id = df_stores[df_stores['Store Name'] == store]['Store ID'].iloc[0]
        df_ranking = df_ranking[df_ranking['STORE ID'] == store_id]

    return df_orders, df_revenue, df_users, df_avg_products, df_avg_revenue, df_avg_users, df_ranking, df_median_views, df_excess_sales, df_stores

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
        <div style="background-color: #f4f1ff; border-radius: 10px; padding: 20px; text-align: center; height: 150px; margin-bottom: 20px;">
            <h3 style="color: #5f4b8b; margin-bottom: 10px;">{title}</h3>
            <h2 style="color: #c084fc; margin-top: 0; font-size: 24px;">{value}</h2>
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

def display_text_metrics(df_median_views, df_excess_sales):
    """
    Exibe as métricas de mediana de visualizações e produtos vendidos sem estoque.
    """
    st.header('Outras Métricas Importantes')

    create_metric_card(
        'Mediana de Vis Antes de Comprar',
        f"{df_median_views['MEDIAN_VIEWS'].iloc[0]:.2f}"
    )

    create_metric_card(
        'Número de Prod Vendidos Sem Estoque',
        f"{df_excess_sales['Total Excess Sales'].iloc[0]:.0f}"
    )

def main():
    """
    Função principal que organiza o fluxo do dashboard.
    """
    configure_page()
    
    # Carregar os dados de lojas
    df_stores = pd.read_csv('data/Stores.csv')
    store_names = ["Todas"] + df_stores['Store Name'].tolist()

    col1, col2 = st.columns([1, 3])
    with col1:
        store = st.selectbox('Escolha uma loja:', store_names)

    # Carregar os dados com base na loja selecionada
    df_orders, df_revenue, df_users, df_avg_products, df_avg_revenue, df_avg_users, df_ranking, df_median_views, df_excess_sales, _ = fetch_data(store)
    
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

    col_left, col_right = st.columns((2, 1))

    # Exibir ranking de produtos mais visualizados na última hora à esquerda
    with col_left:
        display_ranking(df_ranking)

    # Outras métricas importantes à direita
    with col_right:
        display_text_metrics(df_median_views, df_excess_sales)

if __name__ == "__main__":
    main()
