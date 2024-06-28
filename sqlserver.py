import streamlit as st
import pandas as pd
import snowflake.connector
import pyodbc

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

# Function to create SQL Server connection
def create_sql_server_connection(driver, server, database, user, password):
    conn_str = f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};UID={user};PWD={password}"
    return pyodbc.connect(conn_str)

# Function to create table in Snowflake
def create_table(cursor, table_name, df):
    columns = ', '.join([f'"{col}" STRING' for col in df.columns])
    create_table_sql = f'CREATE OR REPLACE TABLE "{table_name}" ({columns})'
    cursor.execute(create_table_sql)

# Function to upload data directly into the Snowflake table
def upload_data_directly(cursor, table_name, df):
    # Prepare insert query
    columns = ', '.join([f'"{col}"' for col in df.columns])
    insert_sql = f'INSERT INTO "{table_name}" ({columns}) VALUES ({", ".join(["%s"] * len(df.columns))})'

    # Execute bulk insert
    data = [tuple(x) for x in df.to_numpy()]
    cursor.executemany(insert_sql, data)

# Streamlit app
def main():
    st.title('Load Data from SQL Server to Snowflake')

    # Snowflake connection parameters input
    st.header('Snowflake Connection')
    snowflake_account = st.text_input('Snowflake Account', '')
    snowflake_user = st.text_input('Username', '')
    snowflake_password = st.text_input('Password', '', type='password')
    snowflake_database = st.text_input('Database', '')
    snowflake_schema = st.text_input('Schema', '')
    snowflake_warehouse = st.text_input('Warehouse', '')

    # SQL Server connection parameters input
    st.header('SQL Server Connection')
    sql_server_driver = st.text_input('SQL Server Driver', 'ODBC Driver 17 for SQL Server')
    sql_server_server = st.text_input('SQL Server', '')
    sql_server_database = st.text_input('SQL Server Database', '')
    sql_server_user = st.text_input('SQL Server Username', '')
    sql_server_password = st.text_input('SQL Server Password', '', type='password')

    # Input for SQL Server table name
    st.header('SQL Server Table')
    sql_server_table_name = st.text_input('SQL Server Table Name', '')

    # Input for Snowflake table name
    st.header('Snowflake Table')
    snowflake_table_name = st.text_input('Snowflake Table Name', '')

    # Button to load data
    if st.button('Load Data'):
        if sql_server_table_name and snowflake_table_name:
            try:
                # Connect to SQL Server
                sql_conn = create_sql_server_connection(
                    sql_server_driver,
                    sql_server_server,
                    sql_server_database,
                    sql_server_user,
                    sql_server_password
                )

                # Read data from SQL Server table
                query = f'SELECT * FROM {sql_server_table_name}'
                df = pd.read_sql(query, sql_conn)

                # Display fetched data
                st.write("Fetched Data from SQL Server:")
                st.write(df)

                # Connect to Snowflake
                snowflake_conn = create_snowflake_connection(
                    snowflake_user,
                    snowflake_password,
                    snowflake_account,
                    snowflake_warehouse,
                    snowflake_database,
                    snowflake_schema
                )
                cursor = snowflake_conn.cursor()

                create_table(cursor, snowflake_table_name, df)
                upload_data_directly(cursor, snowflake_table_name, df)

                st.success(f"Data uploaded to Snowflake table '{snowflake_table_name}'!")

            except Exception as e:
                st.error(f"Error: {str(e)}")

            finally:
                if 'sql_conn' in locals():
                    sql_conn.close()
                if 'snowflake_conn' in locals():
                    snowflake_conn.close()
        else:
            st.warning("Please enter both SQL Server table name and Snowflake table name.")

if __name__ == "__main__":
    main()
