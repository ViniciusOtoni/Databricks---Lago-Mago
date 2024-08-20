#Projeto:

Projeto replicando um processo de migração de um banco OLTP (modelo transacional) para um Data Lake utilizando Databricks.

DB -> Raw -> Bronze -> Silver -> Gold.

A arquitetura utilizada: (Medallion), sugerida pelo próprio Databricks.

### Setup do Databricks:

-Cluster -> Possui uma variável de ambiente (BLOB_STORAGE_ACCOUNT_KEY) que contém o valor da chave de acesso para o Blob Storage.
-Workflow -> Cada Job possui parâmetros como (tablename, catalog, schema).
-Realizo uma cópia do JSON do Job (Reset) para fazer a orquestração do Workflow de forma local.


### Arquivo .ENV:

Para realizar a orquestração do Workflow de forma local, utilizo duas variáveis de ambiente:

-DATABRICKS_TOKEN: Token gerado no Databricks, utilizado na API do Databricks;
-DATABRICKS_HOST: URL do Databricks para fazer a requisição.

```python

import dotenv
import requests

def reset_job(settings):
    url = f"{DATABRICKS_HOST}/api/2.1/jobs/reset"
    header = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}
    
    resp = requests.post(url=url, headers=header, json=settings)
    return resp
```

### Estrutura de Classes:

-src/lib/ingestion.py

Realizo a criação de duas classes:

Ingestor -> Ingestão full-load dos dados para a camada bronze em Delta table.
IngestorCubo -> Ingestão das tabelas dimensionais (cubos) para a camada gold.





Ingestor -> Ingestão full-load dos dados para a camada bronze em Delta table.
IngestorCubo -> Ingestão das tabelas dimensionais (cubos) para a camada gold.