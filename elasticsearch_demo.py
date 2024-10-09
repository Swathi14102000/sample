from elasticsearch import Elasticsearch
import pandas as pd

# Initialize the Elasticsearch client with the API key
client = Elasticsearch(
    "https://my-elasticsearch-project-b6a2e0.es.us-east-1.aws.elastic.cloud:443",
    api_key="TkFJMlo1SUI4aXJDOG9talFqVFc6bEtkTkRUbXpRMnFkQlRqazdteHpEQQ=="
)

# Function to create an index for employee data
def create_employee_index():
    try:
        # Check if the index already exists
        if client.indices.exists(index="employees"):
            print('Index "employees" already exists.')
            return

        # Create the index with settings and mappings for employee data
        response = client.indices.create(
            index="employees",
            body={
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 1
                },
                "mappings": {
                    "properties": {
                        "employeeid":{"type":"integer"},
                        "name": {"type": "text"},
                        "jobtitle":{"type":"text"},
                        "department":{"type":"text"},
                        "businessunit":{"type":"text"},
                        "gender":{"type":"text"},
                        "age": {"type": "integer"},
                        "ethnicity":{"type":"text"},
                        "hiredate":{"type":"date"},
                        "annualsalary":{"type":"float"},
                        "bonus%":{"type":"integer"},
                        "country":{"type":"text"},
                        "city":{"type":"text"},
                        "Exitdate": {"type": "date"},
                        "isActive": {"type": "boolean"}
                    }
                }
            }
        )

        print('Index "employees" created successfully:', response)
    except Exception as e:
        print('Error creating index:', e)

def index_employee_data(csv_file, encoding='ISO-8859-1'):
    try:
        # Read the CSV file into a DataFrame
        df = pd.read_csv(csv_file, encoding = encoding)
        df = df.where(pd.notnull(df), None)
        # Index each employee data one by one
        for index, row in df.iterrows():
            employee = {
                "employeeid":row['Employee ID'],
                "fullname": row['Full Name'],
                "jobtitle":row['Job Title'],
                "department": row['Department'],
                "businessunit":row['Business Unit'],
                "gender":row['Gender'],
                "ethnicity":row['Ethnicity'],
                "age": row['Age'],
                "hiredate":row['Hire Date'],
                "annualsalary":row['Annual Salary'],
                "bonus%":row['Bonus %'],
                "country":row['Country'],
                "city":row['City'],
                "exitdate": row['Exit Date']
            }

            response = client.index(
                index='employees',  # Index name
                body=employee       # Employee data to index
            )

            print(f'Indexed employee: {employee["employeeid"],employee["fullname"],employee["jobtitle"],employee["department"],employee["businessunit"],employee["gender"],employee["ethnicity"],employee[ "age"],employee["hiredate"],employee["annualsalary"],employee["bonus%"],employee["country"],employee["city"],employee["exitdate"]}', response['result'])

        # Refresh the index to make the indexed documents available for search
        client.indices.refresh(index='employees')
        print('Employee data indexed successfully.')
    except Exception as e:
        print('Error indexing employee data:', e)

# Call the functions
create_employee_index()
index_employee_data('EmployeeSampleData1.csv', encoding='ISO-8859-1')
