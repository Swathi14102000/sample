# Define the collection (index) name
from elasticsearch import Elasticsearch
import pandas as pd

# Initialize the Elasticsearch client with the API key
client = Elasticsearch(
    "https://my-elasticsearch-project-b6a2e0.es.us-east-1.aws.elastic.cloud:443",
    api_key="TkFJMlo1SUI4aXJDOG9talFqVFc6bEtkTkRUbXpRMnFkQlRqazdteHpEQQ=="
)

v_nameCollection = 'Hash_Swathi'  # Replace 'YourName' with your actual name

def create_index(es_client, Hash_Swathi):
    index_body = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        },
        "mappings": {
            "properties": {
                "title": {
                    "employee": "text"
                },
                "description": {
                    "type": "text"
                },
                "timestamp": {
                    "type": "date"
                }
                # Add more fields as needed
            }
        }
    }
    
    try:
        if not es_client.indices.exists(index=Hash_Swathi):
            es_client.indices.create(Hash_Swathi, body=index_body)
            print(f"Index '{Hash_Swathi}' created successfully.")
        else:
            print(f"Index '{Hash_Swathi}' already exists.")
    except exceptions.RequestError as re:
        print(f"Request error: {re.info}")
    except Exception as e:
        print(f"An error occurred while creating the index: {e}")
