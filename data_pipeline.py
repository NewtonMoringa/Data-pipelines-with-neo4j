# Import required libraries
from neo4j import GraphDatabase
import pandas as pd
import psycopg2

# Define Neo4j connection details
neo4j_uri = "neo4j+s://f7fda941.databases.neo4j.io"
neo4j_user = "neo4j"
neo4j_password = "hsYhN8uXenrCRxxcyvT8S-MQ0dOKd5r76NNIfRtlBlk"

# Define Postgres connection details
pg_host = "localhost"
pg_database = "telecom_data"
pg_user = "postgres"
pg_password = "password"

# Define Neo4j query to extract data
neo4j_query = 'MATCH (n) RETURN n'


# Define function to extract data from Neo4j and return a Pandas DataFrame
def extract_data():
    # Connect to Neo4j
    driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
        with driver.session() as session:
        # Execute Cypher query and retrieve results
        result = session.run(neo4j_query)
        # Convert results to pandas DataFrame
        df = pd.DataFrame(result.records, columns=result.keys())
    driver.close()
    return df


# Define function to transform data
def transform_data(df):
    # Convert date fields to datetime objects
    df["start_date"] = pd.to_datetime(df["start_date"])
    
    # Remove null values
    df = df.dropna()
     
    return df

# Define function to load data into Postgres
def load_data(df):
    # Connect to Postgres
    conn = psycopg2.connect(host=pg_host, database=pg_database, user=pg_user, password=pg_password)
    # Create table if it doesn't exist
    with conn.cursor() as cursor:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS telecom_data (
            customer_id INTEGER,
            subscription_id INTEGER,
            service_id INTEGER,
            start_date DATE,
            end_date DATE,
            price FLOAT
        )
        """)
    #Insert data
    try:
        for index, row in df.iterrows():
            cur.execute("""INSERT INTO telecom_data (customer_id, subscription_id, service_id, 
            start_date, end_date, price) 
            VALUES(%s, %s, %s, %s, %s, %s)
            """, row['customer_id'], row['subscription_id'], row['service_id'], row['start_date'], row['end_date'], row['price'])
    except:
        return "Error inserting data"
    conn.commit()
    conn.close()

# Define main function
def main():
    # Extract data from Neo4j
    data = extract_data()
    
    # Transform data using Pandas
    transformed_data = transform_data(data)
    
    # Load data into Postgres
    load_data(transformed_data)

# Call main function
if __name__ == "__main__":
    main()
