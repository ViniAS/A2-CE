import streamlit as st
import pandas as pd
import plotly.express as px  # Importar Plotly Express

# Configurando o layout da página para usar mais espaço horizontal
st.set_page_config(layout="wide")

# Carregar dados
df_orders = pd.read_csv('data/Products_Purchased_Per_Minute.csv')
df_revenue = pd.read_csv('data/Revenue_Per_Minute.csv')
df_users = pd.read_csv('data/Unique_User_Views_Per_Product_Per_Minute.csv')

df_avg_products = pd.read_csv('data/Average_Products_Per_Minute.csv')
df_avg_revenue = pd.read_csv('data/Average_Revenue_Per_Minute.csv')
df_avg_users = pd.read_csv('data/Average_Unique_Users_Per_Product_Per_Minute.csv')

# Função para criar um card de métrica
def metric_card(title, value):
    st.markdown(f"""
        <div style="background-color: #f1f3f6; border-radius: 10px; padding: 10px; text-align: center;">
            <h3 style="color: #0e1117; margin-bottom: 0;">{title}</h3>
            <h2 style="color: #ff4b4b; margin-top: 5px;">{value}</h2>
        </div>
        """, unsafe_allow_html=True)

# Definindo três colunas para os gráficos e métricas
col1, col2, col3 = st.columns(3)

# Número médio de produtos comprados por minuto
with col1:
    metric_card('Média de Produtos Comprados por Minuto', f"{df_avg_products['Average Products Per Minute'].iloc[0]:.2f}")
    fig = px.line(df_orders, x='PURCHASE DATE', y='QUANTITY', title="Produtos Comprados por Minuto")
    fig.update_xaxes(rangeslider_visible=True)
    # Adicionando linha de média
    fig.add_hline(y=df_avg_products['Average Products Per Minute'].iloc[0], line_color="red")
    st.plotly_chart(fig)

# Valor faturado por minuto
with col2:
    metric_card('Média de Faturamento por Minuto', f"R$ {df_avg_revenue['Average Revenue Per Minute'].iloc[0]:.2f}")
    fig = px.line(df_revenue, x='REVENUE DATE', y='REVENUE', title="Faturamento por Minuto")
    fig.update_xaxes(rangeslider_visible=True)
    # Adicionando linha de média
    fig.add_hline(y=df_avg_revenue['Average Revenue Per Minute'].iloc[0], line_color="red")
    st.plotly_chart(fig)

# Número de usuários únicos visualizando cada produto por minuto
with col3:
    metric_card('Média de Usuários Únicos por Minuto', f"{df_avg_users['Average Unique Users Per Product Per Minute'].iloc[0]:.2f}")
    fig = px.line(df_users, x='VIEW DATE', y='UNIQUE USERS', title="Usuários Únicos por Produto por Minuto")
    fig.update_xaxes(rangeslider_visible=True)
    # Adicionando linha de média
    fig.add_hline(y=df_avg_users['Average Unique Users Per Product Per Minute'].iloc[0], line_color="red")
    st.plotly_chart(fig)
