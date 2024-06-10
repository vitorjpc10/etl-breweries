# Pipeline de Dados ETL para Dados de Cervejarias

## Descrição
1. Este projeto extrai dados de cervejarias do endpoint da API <https://api.openbrewerydb.org/breweries>
2. Transforma, limpa os dados, persiste os dados em formatos json e parquet, incluindo uma visualização agregada com a quantidade de cervejarias por tipo e localização.
3. Carrega os dados no banco de dados Postgres para capacidades de consulta adicionais.

## Configuração

### Pré-requisitos
- Git
- Docker
- Docker Compose

### Passos para Executar

1. Clone o repositório:
    ```bash
    git clone https://github.com/vitorjpc10/etl-breweries.git
    ```
2. Mova-se para o repositório recém-clonado:
    ```bash
    cd etl-breweries
    ```

### ETL sem Orquestrador (Imagem Docker Python)

3. Construa e execute os containers Docker:
    ```bash
    docker-compose up --build
    ```

4. Os dados serão extraídos, transformados e carregados no banco de dados PostgreSQL com base na lógica em `scripts/main.py`.

5. Uma vez construído, execute o seguinte comando para executar consultas em ambas as tabelas de clima e tráfego do contêiner de banco de dados PostgreSQL:
    ```bash
    docker exec -it etl-breweries-db-1 psql -U postgres -c "\i queries/queries.sql"
    ```

   Digite `\q` no terminal para sair da consulta, há 2 consultas no total.

### ETL com Orquestrador (Apache Airflow)

4. Mova-se para o diretório do Airflow:
    ```bash
    cd airflow
    ```

5. Construa e execute os containers Docker:
    ```bash
    docker-compose up aiflow-init --build
    ```
   ```bash
    docker-compose up
    ```

6. Uma vez que todos os contêineres são construídos, acesse localmente (http://localhost:8080/) e acione o DAG etl_dag (o nome de usuário e a senha são admin por padrão)

7. Uma vez que o DAG é compilado com sucesso, execute o seguinte comando para executar consultas em ambas as tabelas de clima e tráfego:
    ```bash
    docker exec -it airflow-postgres-1 psql -U airflow -c "\i queries/queries.sql"
    ```
   Digite `\q` no terminal para sair da consulta, há 2 consultas no total.

## Suposições e Decisões de Projeto
- O projeto utiliza Docker e Docker Compose para containerização e orquestração, garantindo ambientes de desenvolvimento e implantação consistentes.
- Volumes do Docker são utilizados para persistir os dados do PostgreSQL, garantindo que os dados permaneçam intactos mesmo se os contêineres forem parados ou removidos.
- O banco de dados PostgreSQL é selecionado para armazenamento de dados devido à sua confiabilidade, escalabilidade e suporte para consultas SQL.
- Python puro, SQL e PySpark são usados para manipulação de dados para garantir processamento de dados leve e eficiente.
- As consultas SQL para gerar relatórios são armazenadas em arquivos separados (por exemplo, `queries.sql`). Isso permite fácil modificação das consultas e fornece uma maneira conveniente de visualizar os resultados.
- Para gerar os relatórios, as consultas SQL são executadas dentro do contêiner do PostgreSQL. Esta abordagem simplifica o processo e garante que as consultas possam ser facilmente executadas e modificadas conforme necessário.
- Os dados extraídos são salvos localmente e montados nos contêineres, incluindo os dados brutos provenientes da API e os metadados transformados, ambos em formato JSON e Parquet. Esta configuração oferece simplicidade (princípio KISS) e flexibilidade, permitindo fácil acesso aos dados.
- Uma visualização agregada com a quantidade de cervejarias por tipo e localização é criada para fornecer insights sobre os dados.
- A orquestração por meio do Apache Airflow garante a separação de tarefas e estabelece um framework para executar e monitorar o processo ETL. Ele fornece alertas de notificação para retentativas ou falhas de tarefas, aprimorando a robustez do pipeline.

## Exemplo de DAG do Airflow
![img.png](img.png)
