import pandas as pd
import numpy as np
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from datetime import datetime
import os

today = datetime.now().strftime("%Y%m%d")
input_path = f"/home/saiki_fdiq7b5/datalake/combined/{today}/combined.parquet"
index_name = "weather_disasters"

print(f"✅ Reading: {input_path}")
df = pd.read_parquet(input_path)
print(f"✅ Rows: {len(df)} | Columns: {list(df.columns)}")

# Clean NaN and convert to None (Elasticsearch doesn't accept NaN)
df = df.replace({np.nan: None})

# Create Elasticsearch client
es = Elasticsearch("http://localhost:9200")

# Delete index if exists
if es.indices.exists(index=index_name):
    es.indices.delete(index=index_name)
    print(f"⚠️ Deleted existing index: {index_name}")

# Generator for documents
def doc_generator(df):
    for i, row in df.iterrows():
        yield {
            "_index": index_name,
            "_source": row.to_dict()
        }

# Bulk insert
success, _ = bulk(es, doc_generator(df))
print(f"✅ Indexed {len(df)} documents into '{index_name}'")
