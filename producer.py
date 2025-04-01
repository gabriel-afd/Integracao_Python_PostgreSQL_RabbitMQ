import pika
import json

tabela = input("Para qual tabela deseja importar os dados (operadoras ou demonstrativos_contabeis)? ").strip()
csv_path = input("Informe o caminho do arquivo CSV (copie o caminho do CSV): ").strip()

message = {
    "tabela": tabela,
    "arquivo": csv_path
}

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='csv_import')

channel.basic_publish(
    exchange='',
    routing_key='csv_import',
    body=json.dumps(message)
)

print(f"Mensagem enviada com a tabela '{tabela}' e o caminho: {csv_path}")
connection.close()
