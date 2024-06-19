import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Configurando a seed para reprodutibilidade
np.random.seed(0)

# Definindo o intervalo de data e hora
start = datetime.now()
end = start + timedelta(days=10)
date_range = pd.date_range(start, end, freq='T')

# Gerando dados simulados para número de produtos comprados por minuto
quantities = np.random.randint(1, 10, size=len(date_range))
df_orders = pd.DataFrame({
    "PURCHASE DATE": date_range,
    "QUANTITY": quantities
})

# Gerando dados simulados para número de usuários únicos visualizando cada produto por minuto
product_ids = np.random.randint(1, 101, size=len(date_range))  # 100 produtos diferentes
user_ids = np.random.randint(1, 501, size=len(date_range))     # 500 usuários diferentes
df_user_behaviour = pd.DataFrame({
    "VIEW DATE": date_range,
    "PRODUCT ID": product_ids,
    "USER ID": user_ids
})
grouped_user_views = df_user_behaviour.groupby(['VIEW DATE', 'PRODUCT ID']).agg({'USER ID': pd.Series.nunique}).reset_index()
grouped_user_views.rename(columns={'USER ID': 'UNIQUE USERS'}, inplace=True)

# Gerando dados simulados para valor faturado por minuto
revenue = np.random.uniform(100, 1000, size=len(date_range))
df_revenue = pd.DataFrame({
    "REVENUE DATE": date_range,
    "REVENUE": revenue
})
grouped_revenue = df_revenue.groupby('REVENUE DATE').agg({'REVENUE': np.sum}).reset_index()

# Caminhos dos arquivos CSV
csv_path_orders = 'data/Products_Purchased_Per_Minute.csv'
csv_path_user_views = 'data/Unique_User_Views_Per_Product_Per_Minute.csv'
csv_path_revenue = 'data/Revenue_Per_Minue.csv'

df_orders['PURCHASE DATE'] = df_orders['PURCHASE DATE'].dt.floor('T')
grouped_user_views['VIEW DATE'] = grouped_user_views['VIEW DATE'].dt.floor('T')
grouped_revenue['REVENUE DATE'] = grouped_revenue['REVENUE DATE'].dt.floor('T')

# Salvando os DataFrames em arquivos CSV
df_orders.to_csv(csv_path_orders, index=False)
grouped_user_views.to_csv(csv_path_user_views, index=False)
grouped_revenue.to_csv(csv_path_revenue, index=False)

# Média de produtos comprados por minuto
average_products_per_minute = df_orders['QUANTITY'].mean()

# Média de valor faturado por minuto
average_revenue_per_minute = grouped_revenue['REVENUE'].mean()

# Média de usuários únicos por produto por minuto
average_unique_users_per_product_per_minute = grouped_user_views['UNIQUE USERS'].mean()

# Criando DataFrames para cada média
df_average_products = pd.DataFrame({'Average Products Per Minute': [average_products_per_minute]})
df_average_revenue = pd.DataFrame({'Average Revenue Per Minute': [average_revenue_per_minute]})
df_average_users = pd.DataFrame({'Average Unique Users Per Product Per Minute': [average_unique_users_per_product_per_minute]})

# Caminhos dos arquivos CSV
csv_path_avg_products = 'data/Average_Products_Per_Minute.csv'
csv_path_avg_revenue = 'data/Average_Revenue_Per_Minute.csv'
csv_path_avg_users = 'data/Average_Unique_Users_Per_Product_Per_Minute.csv'

# Salvando os DataFrames em arquivos CSV
df_average_products.to_csv(csv_path_avg_products, index=False)
df_average_revenue.to_csv(csv_path_avg_revenue, index=False)
df_average_users.to_csv(csv_path_avg_users, index=False)

# Gerando IDs de produtos, nomes de produtos, IDs de lojas e nomes de lojas
product_ids = np.arange(1, 101)
product_names = [f"Product {i}" for i in product_ids]
store_ids = np.random.randint(1, 11, size=len(product_ids))
store_names = [f"Store {i}" for i in store_ids]

# Criando um DataFrame de produtos e lojas
df_products = pd.DataFrame({
    "PRODUCT ID": product_ids,
    "PRODUCT NAME": product_names,
    "STORE ID": store_ids,
    "STORE NAME": store_names
})

# Gerando visualizações em uma única hora
view_date_range = pd.date_range(start, periods=60, freq='T')  # Última hora
product_ids_hourly = np.random.choice(product_ids, size=len(view_date_range))

# Criando o DataFrame de visualizações na última hora
df_last_hour_views = pd.DataFrame({
    "VIEW DATE": view_date_range,
    "PRODUCT ID": product_ids_hourly
})

# Agrupando e contando visualizações por produto por minuto
minute_views_count = df_last_hour_views.groupby(['VIEW DATE', 'PRODUCT ID']).size().reset_index(name='VIEW COUNT')

# Agrupando novamente para sumarizar as contagens totais por produto ao longo da hora
total_minute_views = minute_views_count.groupby('PRODUCT ID').agg({'VIEW COUNT': 'sum'}).reset_index()

# Ordenando para obter o ranking dos produtos mais visualizados
ranked_minute_products = total_minute_views.sort_values(by='VIEW COUNT', ascending=False).reset_index(drop=True)

# Adicionando informações dos produtos e das lojas ao ranking
ranked_minute_products = ranked_minute_products.merge(df_products, on='PRODUCT ID')

# Salvando em um arquivo CSV
csv_path_minute_hourly_views = 'data/Most_Viewed_Products_Per_Minute_Last_Hour.csv'
ranked_minute_products.to_csv(csv_path_minute_hourly_views, index=False)


# Simulando dados para a pergunta sobre a mediana do número de vezes que um usuário visualiza um produto antes de efetuar uma compra

# Gerando IDs de produtos e usuários, e se foi compra ou visualização
view_purchase_events = pd.date_range(start, periods=1000, freq='T')
user_ids_events = np.random.randint(1, 501, size=len(view_purchase_events))
product_ids_events = np.random.randint(1, 101, size=len(view_purchase_events))
event_types = np.random.choice(['view', 'purchase'], size=len(view_purchase_events), p=['0.8', '0.2'])  # 80% view, 20% purchase

# Criando o DataFrame
df_events = pd.DataFrame({
    "EVENT DATE": view_purchase_events,
    "USER ID": user_ids_events,
    "PRODUCT ID": product_ids_events,
    "EVENT TYPE": event_types
})

# Redefinindo variáveis para a geração de dados e simulando novamente
view_purchase_events = pd.date_range(start, periods=1000, freq='T')
user_ids_events = np.random.randint(1, 501, size=len(view_purchase_events))
product_ids_events = np.random.randint(1, 101, size=len(view_purchase_events))
event_types = np.random.choice(['view', 'purchase'], size=len(view_purchase_events), p=[0.8, 0.2])  # 80% view, 20% purchase

# Criando o DataFrame de eventos novamente
df_events = pd.DataFrame({
    "EVENT DATE": view_purchase_events,
    "USER ID": user_ids_events,
    "PRODUCT ID": product_ids_events,
    "EVENT TYPE": event_types
})


# Gerando dados de vendas e estoque
sold_product_ids = np.random.randint(1, 101, size=500)  # 500 vendas
sold_quantities = np.random.randint(1, 10, size=500)  # Quantidades vendidas
stock_quantities = np.random.randint(-5, 15, size=100)  # Estoque pode variar de -5 a 15

# Criando DataFrames para vendas e estoque
df_sales = pd.DataFrame({
    "PRODUCT ID": sold_product_ids,
    "SOLD QUANTITY": sold_quantities
})
df_stock = pd.DataFrame({
    "PRODUCT ID": range(1, 101),
    "STOCK QUANTITY": stock_quantities
})

# Agrupando vendas por produto e somando as quantidades
grouped_sales = df_sales.groupby('PRODUCT ID').agg({'SOLD QUANTITY': np.sum}).reset_index()

# Unindo vendas e estoque para calcular os produtos vendidos sem disponibilidade
sales_with_stock = grouped_sales.merge(df_stock, on='PRODUCT ID', how='left')
sales_with_stock['EXCESS SALES'] = sales_with_stock.apply(
    lambda x: max(0, x['SOLD QUANTITY'] - max(0, x['STOCK QUANTITY'])), axis=1
)

# Calculando o total de vendas sem disponibilidade no estoque
total_excess_sales = sales_with_stock['EXCESS SALES'].sum()

# Salvando o resultado em um arquivo CSV
csv_path_total_excess_sales = 'data/Total_Excess_Sales.csv'
pd.DataFrame({"Total Excess Sales": [total_excess_sales]}).to_csv(csv_path_total_excess_sales, index=False)

# Exibindo o caminho do arquivo e o valor total
csv_path_total_excess_sales, total_excess_sales