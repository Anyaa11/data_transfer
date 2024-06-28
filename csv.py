import streamlit as st
import pandas as pd
import snowflake.connector
import tempfile
import os
import csv

# Function to create Snowflake connection
def create_snowflake_connection(user, password, account, warehouse, database, schema):
    return snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema
    )

# Function to create table in Snowflake
def create_table(cursor, table_name, df):
    columns = ', '.join([f'"{col}" STRING' for col in df.columns])
    create_table_sql = f'CREATE OR REPLACE TABLE "{table_name}" ({columns})'
    cursor.execute(create_table_sql)

# Function to upload data directly into the Snowflake table
def upload_data_directly(cursor, table_name, df):
    # Save the dataframe to a temporary file
    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmpfile:
        df.to_csv(tmpfile.name, index=False, header=False)
        tmpfile_path = tmpfile.name

    # Read the CSV file content
    with open(tmpfile_path, 'r') as file:
        reader = csv.reader(file)
        rows = [tuple(row) for row in reader]

    # Prepare insert query
    columns = ', '.join([f'"{col}"' for col in df.columns])
    insert_sql = f'INSERT INTO "{table_name}" ({columns}) VALUES ({", ".join(["%s"] * len(df.columns))})'

    # Execute bulk insert
    cursor.executemany(insert_sql, rows)

    # Clean up the temporary file
    os.remove(tmpfile_path)

# Streamlit app
def main():
    st.title('Upload CSV to Snowflake')

    # Snowflake connection parameters input
    st.header('Snowflake Connection')
    snowflake_account = st.text_input('Snowflake Account', '')
    snowflake_user = st.text_input('Username', '')
    snowflake_password = st.text_input('Password', '', type='password')
    snowflake_database = st.text_input('Database', '')
    snowflake_schema = st.text_input('Schema', '')
    snowflake_warehouse = st.text_input('Warehouse', '')

    # Upload CSV file
    st.header('Upload CSV File')
    uploaded_file = st.file_uploader("Choose a CSV file")

    if uploaded_file is not None:
        # Read CSV file
        df = pd.read_csv(uploaded_file)

        # Display uploaded data
        st.write("Uploaded Data:")
        st.write(df)

        # Input for table name
        table_name = st.text_input('Table Name', '')

        # Button to load data
        if st.button('Load Data'):
            if table_name:
                try:
                    conn = create_snowflake_connection(
                        snowflake_user,
                        snowflake_password,
                        snowflake_account,
                        snowflake_warehouse,
                        snowflake_database,
                        snowflake_schema
                    )
                    cursor = conn.cursor()

                    create_table(cursor, table_name, df)
                    upload_data_directly(cursor, table_name, df)

                    st.success(f"Data uploaded to Snowflake table '{table_name}'!")

                except Exception as e:
                    st.error(f"Error: {str(e)}")

                finally:
                    if 'conn' in locals():
                        conn.close()

            else:
                st.warning("Please enter a table name.")

if __name__ == "__main__":
    main()
