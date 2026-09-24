# MVP de Engenharia de Dados — Brazilian E-Commerce (Olist)

**Nome:** Lucas Dantas Matos Vidal  
**Matrícula:** 4052025002493  

Projeto desenvolvido como MVP da disciplina de Engenharia de Dados da PUC, utilizando o dataset público **Brazilian E-Commerce Public Dataset by Olist**.

O objetivo é construir um pipeline de dados na nuvem, desde a ingestão dos arquivos CSV até a disponibilização de tabelas analíticas para responder a perguntas de negócio sobre pedidos, vendas, produtos, clientes e entregas.

## 1. Tecnologias utilizadas

- Databricks Free Edition
- Apache Spark e PySpark
- Spark SQL
- Delta Lake
- Unity Catalog
- GitHub


## 2. Fonte dos dados

Foi utilizado o **Brazilian E-Commerce Public Dataset by Olist**, disponibilizado no Kaggle.

Dataset: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

**Licença dos dados:** CC BY-NC-SA 4.0

O conjunto contém dados históricos de comércio eletrônico brasileiro, incluindo pedidos, itens, pagamentos, avaliações, produtos, clientes, vendedores e informações geográficas. Os dados se concentram principalmente no período de 2016 a 2018.

Os arquivos originais não foram incluídos neste repositório. A fonte e as condições de uso devem ser consultadas na página do dataset.

## 3. Arquitetura da solução

O projeto foi desenvolvido no Databricks seguindo a arquitetura **Medallion**, com três camadas:

```text
Arquivos CSV da Olist
        |
        v
      BRONZE
Ingestão dos dados originais
em tabelas Delta
        |
        v
      SILVER
Limpeza, padronização e
tratamento da qualidade
        |
        v
       GOLD
Modelo dimensional para
consultas e análises
        |
        v
   ANÁLISES SQL
Indicadores e perguntas
de negócio
```

Os dados são armazenados em tabelas Delta nos schemas `bronze`, `silver` e `gold` do catálogo `workspace`.

## 4. Pipeline de dados

### Camada Bronze

Ingestão dos nove arquivos CSV originais da Olist e persistência em tabelas Delta. Também foram realizadas verificações iniciais de qualidade, incluindo valores nulos, duplicidades, integridade referencial e consistência dos dados.

### Camada Silver

Tratamento e padronização dos dados para consumo analítico, incluindo limpeza de registros e remoção de duplicidades identificadas.

### Camada Gold

Construção de um modelo dimensional com quatro dimensões e três tabelas fato.

**Dimensões:**
- `dim_clientes`
- `dim_produtos`
- `dim_tempo`
- `dim_vendedores`

**Fatos:**
- `fato_pedidos`
- `fato_itens_pedido`
- `fato_pagamentos`

As tabelas e colunas da camada Gold foram documentadas no Unity Catalog.

## 5. Análises realizadas

As consultas SQL exploram questões como:

- Evolução mensal das vendas e dos pedidos.
- Ticket médio ao longo do tempo.
- Categorias de produtos com maior participação nas vendas.
- Distribuição das vendas por estado.
- Situação dos pedidos.
- Relação entre prazo de entrega e avaliações dos clientes.

## 6. Organização do repositório

Os notebooks estão disponíveis na pasta [`notebooks/`](notebooks/):

| Notebook | Descrição |
|---|---|
| `00_setup_ambiente.ipynb` | Configuração inicial do ambiente |
| `01_ingestao_bronze.ipynb` | Ingestão dos arquivos CSV na camada Bronze |
| `02_qualidade_bronze.ipynb` | Verificações de qualidade dos dados Bronze |
| `03_transformacao_silver.ipynb` | Limpeza e transformação para a camada Silver |
| `04_modelagem_gold.ipynb` | Construção do modelo dimensional Gold |
| `05_catalogo_dados.ipynb` | Documentação das tabelas e colunas |
| `06_analise_exploratoria_olist.ipynb` | Consultas e análises exploratórias |

## 7. Como reproduzir o projeto

1. Obter os arquivos CSV na página do dataset da Olist no Kaggle.
2. Criar um ambiente no Databricks com acesso a Spark e Delta Lake.
3. Disponibilizar os arquivos no volume `/Volumes/workspace/bronze/olist_raw`, ou adaptar o caminho no notebook de ingestão.
4. Executar os notebooks na ordem numérica, de `00` a `06`.

**Observação:** os arquivos `.ipynb` documentam o desenvolvimento e podem incluir resultados de execuções anteriores. Para reproduzir o pipeline, é necessário configurar o ambiente e disponibilizar os dados de origem.

