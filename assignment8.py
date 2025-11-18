from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra import ConsistencyLevel
import csv
import os
import sys

# --- Configuration Values (BASED ON YOUR INPUTS) ---

# 1. Download your Secure Connect Bundle (Path must be ABSOLUTE)
# *** YOU MUST UPDATE THIS PATH TO YOUR ABSOLUTE LOCAL .ZIP FILE PATH ***
SECURE_CONNECT_BUNDLE_PATH = "secure-connect-sarthakdb.zip" 

# 2. Token Details (Split for PlainTextAuthProvider):
# Client ID (Username)
ASTRA_CLIENT_ID_PLACEHOLDER = "ubSQbRPeGaaFmCeALCIuAwJQ"  
# Secret (Password)
ASTRA_TOKEN_SECRET = "waxjE68mJPtbJGGp,c35IPhlU_1a7-bU4m1xTjkJUal1CU3bRpwfbED5gdr_L00FZ0JkEMRmZllSJ,ZlJiczu+hRhZkuXknGcP,PZfc9Txx2uugTLTWGk8T,OjFKjgJH" 

# Your Keyspace Name
KEYSPACE_NAME = "default_keyspace" 
# --------------------------------------------------

class CassandraDB():
    """
    Handles connection, table creation, data loading, and queries for Cassandra.
    """

    def __init__(self):
        self.session = None
        self.cluster = None
        # Actual columns from customers.csv
        self.CSV_FIELDS = ['id', 'gender', 'age', 'number_of_kids'] 
        self.CSV_FILE = "data/customers.csv" 

    # ----------------------------------------------------------------------
    # Task 1: connect()
    # ----------------------------------------------------------------------
    def connect(self):
        """Connects to the DataStax Astra DB using the Secure Connect Bundle."""
        print("Connecting to DataStax Astra DB...")
        try:
            # 1. Check for bundle path existence 
            if not os.path.exists(SECURE_CONNECT_BUNDLE_PATH):
                 raise FileNotFoundError(f"Secure connect bundle not found at: {SECURE_CONNECT_BUNDLE_PATH}")

            cloud_config= {
                'secure_connect_bundle': SECURE_CONNECT_BUNDLE_PATH
            }
            # 2. Authenticate using the Client ID (Username) and the Secret (Password)
            auth_provider = PlainTextAuthProvider(ASTRA_CLIENT_ID_PLACEHOLDER, ASTRA_TOKEN_SECRET)
            
            # 3. Initialize the Cluster
            self.cluster = Cluster(
                cloud=cloud_config,
                auth_provider=auth_provider
            )
            
            # 4. Connect to the Keyspace
            self.session = self.cluster.connect(KEYSPACE_NAME)
            print(f"Successfully connected to Keyspace: {KEYSPACE_NAME}")

        except Exception as e:
            # The previous timeout/authentication error should be caught here if credentials fail.
            print(f"Connection Error: {e}")
            print("Action Required: Check absolute SECURE_CONNECT_BUNDLE_PATH & ensure database is Active.")
            self.session = None

    def close(self):
        """Closes the Cassandra connection."""
        if self.cluster:
            self.cluster.shutdown()

    # ----------------------------------------------------------------------
    # Task 2: create_table()
    # ----------------------------------------------------------------------
    def create_table(self):
        """Creates the 'Customer' table and necessary indices."""
        if not self.session:
            print("Cannot create table: Not connected to the database.")
            return

        # 'id' as PRIMARY KEY (int) for fast lookup (Query 1)
        CREATE_TABLE_QUERY = f"""
            CREATE TABLE IF NOT EXISTS {KEYSPACE_NAME}.customer (
                id int,             
                gender text,        
                age int,            
                number_of_kids int, 
                PRIMARY KEY (id) 
            );
            """
        # Secondary Indices for Query 2 filtering
        CREATE_INDEX_GENDER = f"""
            CREATE INDEX IF NOT EXISTS on {KEYSPACE_NAME}.customer (gender);
        """
        CREATE_INDEX_AGE = f"""
            CREATE INDEX IF NOT EXISTS on {KEYSPACE_NAME}.customer (age);
        """

        try:
            self.session.execute(CREATE_TABLE_QUERY)
            self.session.execute(CREATE_INDEX_GENDER)
            self.session.execute(CREATE_INDEX_AGE)
            print("Table 'customer' and indices created.")
            
        except Exception as e:
            print(f"Table Creation Error: {e}")

    # ----------------------------------------------------------------------
    # Task 3: load_data()
    # ----------------------------------------------------------------------
    def load_data(self):
        """Loads data from data/customers.csv into the Customer table."""
        if not self.session:
            print("Cannot load data: Not connected to the database.")
            return

        print(f"Loading data from {self.CSV_FILE}...")
        
        INSERT_CQL = f"INSERT INTO {KEYSPACE_NAME}.customer (id, gender, age, number_of_kids) VALUES (?, ?, ?, ?)"
        
        try:
            prepared_stmt = self.session.prepare(INSERT_CQL) 
            rows_to_insert = []
            
            with open(self.CSV_FILE, mode='r', encoding='utf-8') as file:
                csv_reader = csv.DictReader(file, fieldnames=self.CSV_FIELDS)
                
                # Check and handle header line
                first_row = next(csv_reader)
                if str(first_row.get('id', '')).strip() != 'id':
                    try:
                        rows_to_insert.append((
                            int(first_row['id']),
                            first_row['gender'].strip(),
                            int(first_row['age']),
                            int(first_row['number_of_kids'])
                        ))
                    except ValueError:
                        pass
                
                for row in csv_reader:
                    try:
                        rows_to_insert.append((
                            int(row['id']),
                            row['gender'].strip(),
                            int(row['age']),
                            int(row['number_of_kids'])
                        ))
                    except ValueError as ve:
                        if row.get('id'):
                            print(f"Skipping row due to data conversion error: {ve}. Row data: {row}")
                        continue
            
            for row_data in rows_to_insert:
                self.session.execute(prepared_stmt, row_data)
            
            print(f"Successfully loaded {len(rows_to_insert)} records into the customer table.")

        except FileNotFoundError:
            print(f"Data Loading Error: CSV file '{self.CSV_FILE}' not found. Ensure it is placed in the correct path.")
        except Exception as e:
            print(f"Data Loading Error: {e}")
    
    # ----------------------------------------------------------------------
    # Task 4: query_1()
    # ----------------------------------------------------------------------
    def query_1(self):
        """Returns the age of the customer whose id is 979863."""
        if not self.session:
            print("Cannot run query: Not connected to the database.")
            return

        CUSTOMER_ID = 979863
        QUERY_CQL = f"SELECT age FROM {KEYSPACE_NAME}.customer WHERE id = %s"
        
        try:
            print(f"\n--- Task 4: Query 1 (Age of customer ID {CUSTOMER_ID}) ---")
            result = self.session.execute(QUERY_CQL, (CUSTOMER_ID,))
            
            row = result.one()
            if row:
                print(f"Result: Age = {row.age}")
                return row.age
            else:
                print("Result: Customer not found.")
                return None

        except Exception as e:
            print(f"Query 1 Error: {e}")
            return None

    # ----------------------------------------------------------------------
    # Task 5: query_2()
    # ----------------------------------------------------------------------
    def query_2(self):
        """Returns information of customers who are 'MALE' and age is 25 or 35."""
        if not self.session:
            print("Cannot run query: Not connected to the database.")
            return

        QUERY_CQL = f"""
            SELECT id, gender, age, number_of_kids FROM {KEYSPACE_NAME}.customer 
            WHERE gender = 'MALE' AND age IN (25, 35)
            ALLOW FILTERING; 
        """

        try:
            print(f"\n--- Task 5: Query 2 (MALE, age 25 or 35) ---")
            
            results = self.session.execute(QUERY_CQL)
            
            customer_list = list(results)
            
            print(f"Result: Found {len(customer_list)} matching customers.")
            for row in customer_list:
                 print(f"  ID: {row.id}, Age: {row.age}, Gender: {row.gender}, Kids: {row.number_of_kids}")
                 
            return customer_list

        except Exception as e:
            print(f"Query 2 Error: {e}")
            return []


if __name__ == '__main__':
    
    # Check if placeholder path is used
    if SECURE_CONNECT_BUNDLE_PATH == r"C:\path\to\secure-connect-sarthakdb.zip":
        print("\n*** ACTION REQUIRED ***: Please replace the placeholder path for SECURE_CONNECT_BUNDLE_PATH in the script.")
    else:
        client = CassandraDB()
        client.connect() # Task 1
        
        if client.session:
            client.create_table() # Task 2
            client.load_data()    # Task 3
            
            client.query_1()    # Task 4
            client.query_2()    # Task 5
            
        client.close()