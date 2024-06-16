import pika
import json
import time
from collections import deque

# Definir as condições para os cupons
# Exemplo: [(total_value, time_interval_in_seconds)]
CONDITIONS = [
    (500, 1800/3),  # 500 unidades monetárias nos últimos 30 minutos
    (2000, 3600*6)  # 1000 unidades monetárias na última hora
]

# Função para conectar ao RabbitMQ
def connect_to_rabbitmq():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    return connection, channel

# Função para processar a mensagem de compra
def process_purchase(channel, method, properties, body):
    purchase = json.loads(body)
    user_id = purchase['user_id']
    value = purchase['value']
    timestamp = purchase['timestamp']
    store_id = purchase['store_id']
    print(f"Recebido: {purchase}")
    
    # Processar a compra aqui (exemplo com deque para janela deslizante)
    if store_id not in store_purchases:
        store_purchases[store_id] = {}
    if user_id not in store_purchases[store_id]:
        store_purchases[store_id][user_id] = deque()

    # Adicionar compra ao deque
    store_purchases[store_id][user_id].append(purchase)
    
    # Verificar condições
    if verificar_condicoes(store_id, user_id):
        compras_relevantes = listar_compras_relevantes(store_id, user_id)
        print(f"Gerar cupom para usuário {user_id} na loja {store_id}")
        print("Compras utilizadas para gerar o cupom:", compras_relevantes)
        # Informar o produtor sobre a geração do cupom
        informar_produtor(user_id, store_id)
        # Registrar no log
        registrar_cupom(user_id, store_id, compras_relevantes)
        # Limpar compras do usuário
        store_purchases[store_id][user_id].clear()

# Função para verificar as condições dos cupons
def verificar_condicoes(store_id, user_id):
    current_time = int(time.time())
    for total_value, interval in CONDITIONS:
        total = 0
        print(f"Verificando condição: valor total {total_value}, intervalo {interval} segundos")
        for purchase in store_purchases[store_id][user_id]:
            if purchase['timestamp'] >= current_time - interval:
                total += purchase['value']
        print(f"Total acumulado: {total} (necessário: {total_value})")
        # Se alguma condição não for atendida, retornar False
        if total < total_value:
            return False
    # Todas as condições foram atendidas
    return True

# Função para listar as compras relevantes que atenderam as condições
def listar_compras_relevantes(store_id, user_id):
    current_time = int(time.time())
    compras_relevantes = []
    for purchase in store_purchases[store_id][user_id]:
        for total_value, interval in CONDITIONS:
            if purchase['timestamp'] >= current_time - interval:
                compras_relevantes.append(purchase)
    return compras_relevantes

def informar_produtor(user_id, store_id):
    # Função para notificar o produtor que um cupom foi gerado
    # Enviar uma mensagem para uma fila específica para o produtor
    connection, channel = connect_to_rabbitmq()
    mensagem = {
        'user_id': user_id,
        'store_id': store_id,
        'discount': 10  # 10% de desconto
    }
    channel.queue_declare(queue='cupons_gerados')
    channel.basic_publish(
        exchange='',
        routing_key='cupons_gerados',
        body=json.dumps(mensagem)
    )
    print(f"Notificação enviada ao produtor: {mensagem}")
    connection.close()

def registrar_cupom(user_id, store_id, compras_relevantes):
    # Registrar no log o cupom gerado e as compras utilizadas
    mensagem_log = {
        'user_id': user_id,
        'store_id': store_id,
        'compras_utilizadas': compras_relevantes
    }
    with open('cupons_gerados.txt', 'a') as f:
        f.write(f"{json.dumps(mensagem_log)}\n")

# Função principal para iniciar o consumidor
def main():
    global store_purchases
    store_purchases = {}

    lojas = ['compras_loja1', 'compras_loja2']  # Adicione todas as lojas
    connection, channel = connect_to_rabbitmq()

    for loja in lojas:
        channel.queue_declare(queue=loja)
        # Definir o consumidor como exclusivo
        channel.basic_consume(queue=loja, on_message_callback=process_purchase, auto_ack=True, exclusive=True)

    print(' [*] Esperando por mensagens. Para sair pressione CTRL+C')
    channel.start_consuming()

if __name__ == '__main__':
    main()
