# Projeto de Importação Automatizada de Dados da ANS com RabbitMQ e PostgreSQL

Este projeto tem como objetivo automatizar a importação de dados públicos da ANS (Agência Nacional de Saúde Suplementar) para um banco de dados **PostgreSQL**. A comunicação entre os componentes é feita por meio do **RabbitMQ**. O sistema lê arquivos CSV e insere os dados nas tabelas `operadoras` e `demonstrativos_contabeis` de forma robusta e eficiente.

## Tecnologias Utilizadas

- Python 3.11+
- RabbitMQ
- PostgreSQL
- Docker (opcional, para rodar o RabbitMQ)
- Bibliotecas: `pika`, `pandas`, `psycopg2`

---

## Pré-Requisitos

### 1. PostgreSQL

Você deve ter um banco de dados PostgreSQL rodando com o banco `db_intuitive_care` criado. Dentro desse banco, execute os comandos abaixo para criar as tabelas:

```sql
CREATE TABLE operadoras(
    registro_ans INTEGER PRIMARY KEY,
    cnpj VARCHAR(20),
    razao_social VARCHAR(255),
    nome_fantasia VARCHAR(255),
    modalidade VARCHAR(100),
    logradouro VARCHAR(255),
    numero VARCHAR(20),
    complemento VARCHAR(100),
    bairro VARCHAR(100),
    cidade VARCHAR(100),
    uf VARCHAR(2),
    cep VARCHAR(10),
    ddd VARCHAR(3),
    telefone VARCHAR(30),
    fax VARCHAR(30),
    endereco_eletronico VARCHAR(255),
    representante VARCHAR(255),
    cargo_representante VARCHAR(100),
    regiao_de_comercializacao INTEGER,
    data_registro_ans DATE
);

CREATE TABLE demonstrativos_contabeis(
    id SERIAL PRIMARY KEY,
    data DATE,
    reg_ans INTEGER,
    cd_conta_contabil VARCHAR(20),
    descricao VARCHAR(255),
    vl_saldo_inicial NUMERIC(18,2),
    vl_saldo_final NUMERIC(18,2),
    CONSTRAINT operadoras_demonstrativos_contabeis_fk FOREIGN KEY (reg_ans) 
        REFERENCES operadoras(registro_ans)
);
```

⚠️ No arquivo `consumer.py`, atualize os campos `user`, `password`, `host`, `port` e `database` com suas credenciais do PostgreSQL:
```python
conn = psycopg2.connect(
    user='seu_usuario',
    password='sua_senha',
    host='localhost',
    port='5432',
    database='db_intuitive_care'
)
```

---

### 2. RabbitMQ

Você pode instalar o RabbitMQ localmente ou rodá-lo em um container Docker:

#### 🐳 Rodando com Docker:
```bash
docker run -d --hostname my-rabbit --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```

> Acesse o painel do RabbitMQ em: http://localhost:15672  
> Login padrão: `guest`, senha: `guest`

---

## Instalação das Dependências

Execute o script abaixo em Python para instalar automaticamente as bibliotecas necessárias:

```python
import subprocess
import sys

required_packages = ['pika', 'pandas', 'psycopg2']
for package in required_packages:
    try:
        __import__(package.split('-')[0])
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])
```

---

## Como Rodar o Projeto

### 1. Producer

Executa o script `producer.py` para enviar o caminho do CSV e a tabela desejada (operadoras ou demonstrativos_contabeis):

```bash
python producer.py
```

### 2. Consumer (execute primeiramente consumer.py)

Executa o script `consumer.py` para consumir a mensagem da fila e importar os dados:

```bash
python consumer.py
```

---

## Fontes dos Arquivos CSV

- **Operadoras Ativas**:  
  https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/

- **Demonstrativos Contábeis (últimos 2 anos)**:  
  https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/

---

## Observações

- A importação da tabela `demonstrativos_contabeis` só será bem-sucedida se os valores de `reg_ans` existirem na tabela `operadoras`.
- O sistema está preparado para ignorar valores inválidos ou registros incompletos.
- A inserção em batch garante performance otimizada para grandes volumes de dados.

---

## Link do Vídeo Demonstrando o Código

<a href="https://youtu.be/FkdklRgLreQ" target="_blank">Assista ao vídeo demonstrando o código</a>

---

## Autor

Desenvolvido por Gabriel Medeiros de Mendonça.  
Em caso de dúvidas ou melhorias, sinta-se à vontade para contribuir!
