import pika
import pandas as pd
import psycopg2
import json
import os
from datetime import datetime

# Função para criar conexão com PostgreSQL
def get_postgres_connection():
    return psycopg2.connect(
        host='localhost',
        user='postgres',  # ajuste conforme seu ambiente
        password='85854121',  # ajuste conforme seu ambiente
        dbname='db_intuitive_care'
    )

# Inserção na tabela operadoras
def insert_operadoras_postgres(df):
    conn = get_postgres_connection()
    cursor = conn.cursor()

    insert_query = """
        INSERT INTO operadoras (
            registro_ans, cnpj, razao_social, nome_fantasia, modalidade, logradouro,
            numero, complemento, bairro, cidade, uf, cep, ddd, telefone, fax,
            endereco_eletronico, representante, cargo_representante,
            regiao_de_comercializacao, data_registro_ans
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    for index, row in df.iterrows():
        try:
            registro_ans = row.get('REGISTRO_ANS')
            if pd.isna(registro_ans) or str(registro_ans).strip().lower() == 'nan':
                print(f"[Linha {index}] Ignorada: REGISTRO_ANS inválido.")
                continue

            ddd = row.get('DDD')
            try:
                ddd = str(int(float(ddd))) if pd.notna(ddd) else None
            except:
                ddd = None

            data_raw = row.get('DATA_REGISTRO_ANS')
            data_registro = None
            if pd.notna(data_raw):
                for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%Y/%m/%d']:
                    try:
                        data_registro = datetime.strptime(str(data_raw).strip(), fmt).date()
                        break
                    except:
                        continue

            regiao = row.get('REGIAO_DE_COMERCIALIZACAO')
            try:
                regiao = int(regiao) if pd.notna(regiao) else None
            except:
                regiao = None

            cursor.execute(insert_query, (
                int(registro_ans),
                str(row.get('CNPJ') or '').strip() or None,
                str(row.get('RAZAO_SOCIAL') or '').strip() or None,
                str(row.get('NOME_FANTASIA') or '').strip() or None,
                str(row.get('MODALIDADE') or '').strip() or None,
                str(row.get('LOGRADOURO') or '').strip() or None,
                str(row.get('NUMERO') or '').strip() or None,
                str(row.get('COMPLEMENTO') or '').strip() or None,
                str(row.get('BAIRRO') or '').strip() or None,
                str(row.get('CIDADE') or '').strip() or None,
                str(row.get('UF') or '').strip() or None,
                str(row.get('CEP') or '').strip() or None,
                ddd,
                str(row.get('TELEFONE') or '').strip() or None,
                str(row.get('FAX') or '').strip() or None,
                str(row.get('ENDERECO_ELETRONICO') or '').strip() or None,
                str(row.get('REPRESENTANTE') or '').strip() or None,
                str(row.get('CARGO_REPRESENTANTE') or '').strip() or None,
                regiao,
                data_registro
            ))
        except Exception as e:
            print(f"[Linha {index}] Erro ao inserir: {e}")
            continue

    conn.commit()
    cursor.close()
    conn.close()

# Inserção na tabela demonstrativos_contabeis
def insert_demonstrativos_postgres(df):
    conn = get_postgres_connection()
    cursor = conn.cursor()

    df = df.copy()
    df['REG_ANS'] = pd.to_numeric(df['REG_ANS'], errors='coerce')
    df['VL_SALDO_INICIAL'] = df['VL_SALDO_INICIAL'].astype(str).str.replace(',', '.').astype(float)
    df['VL_SALDO_FINAL'] = df['VL_SALDO_FINAL'].astype(str).str.replace(',', '.').astype(float)
    df['DATA'] = pd.to_datetime(df['DATA'], errors='coerce').dt.date

    cursor.execute("SELECT registro_ans FROM operadoras")
    registros_validos = set(row[0] for row in cursor.fetchall())
    df = df[df['REG_ANS'].isin(registros_validos)]

    insert_query = """
        INSERT INTO demonstrativos_contabeis (
            data, reg_ans, cd_conta_contabil, descricao, vl_saldo_inicial, vl_saldo_final
        ) VALUES (%s, %s, %s, %s, %s, %s)
    """

    values = [
        (
            row['DATA'],
            int(row['REG_ANS']),
            row['CD_CONTA_CONTABIL'],
            row['DESCRICAO'],
            row['VL_SALDO_INICIAL'],
            row['VL_SALDO_FINAL']
        )
        for _, row in df.iterrows()
    ]

    try:
        cursor.executemany(insert_query, values)
        conn.commit()
    except Exception as e:
        print(f"[ERRO] Insert em batch falhou: {e}")

    cursor.close()
    conn.close()

# Função callback para consumir mensagens
def callback(ch, method, properties, body):
    msg = json.loads(body)
    tabela = msg['tabela']
    filePath_csv = msg['arquivo'].strip('"')

    if not os.path.exists(filePath_csv):
        print(f"[ERRO] Arquivo não encontrado: {filePath_csv}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    print(f"[INFO] Importando para a tabela {tabela.upper()}: {filePath_csv}")
    df = pd.read_csv(filePath_csv, sep=';', encoding='utf-8')
    df.columns = [col.strip().upper().replace(' ', '_') for col in df.columns]

    if tabela == 'operadoras':
        insert_operadoras_postgres(df)
    elif tabela == 'demonstrativos_contabeis':
        insert_demonstrativos_postgres(df)

    print("[SUCESSO] Importação concluída!")
    ch.basic_ack(delivery_tag=method.delivery_tag)

# Configurações do RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='csv_import')
channel.basic_consume(queue='csv_import', on_message_callback=callback, auto_ack=False)
print('Mensagens estão sendo geradas..... \nSe quiser sair, pressione CTRL+C')
channel.start_consuming()
