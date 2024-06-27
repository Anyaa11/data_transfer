# requirements.txt
# streamlit
# snowflake-connector-python
# pandas

# app.py
import streamlit as st
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import base64


# Snowflake connection function
def get_snowflake_connection():
    return snowflake.connector.connect(
        user=st.secrets["snowflake"]["user"],
        password=st.secrets["snowflake"]["password"],
        account=st.secrets["snowflake"]["account"],
        warehouse=st.secrets["snowflake"]["warehouse"],
        database=st.secrets["snowflake"]["database"],
        schema=st.secrets["snowflake"]["schema"]
    )


# Function to load CSV data into Snowflake
def load_data_to_snowflake(df, table_name):
    conn = get_snowflake_connection()
    try:
        success, nchunks, nrows, _ = write_pandas(conn, df, table_name.upper())
        st.success(f"Successfully loaded {nrows} rows into {table_name} table.")
    except Exception as e:
        st.error(f"An error occurred: {e}")
    finally:
        conn.close()


# Streamlit app
st.title("CSV to Snowflake Loader")

# Custom HTML and JavaScript for drag-and-drop file upload
st.markdown("""
    <style>
    .file-drop-zone {
        border: 2px dashed #bbb;
        border-radius: 10px;
        padding: 20px;
        text-align: center;
        cursor: pointer;
    }
    </style>
    <div class="file-drop-zone" id="drop-zone">
        Drag and drop your CSV file here
    </div>
    <input type="file" id="file-input" style="display: none;" />
    <script>
    const dropZone = document.getElementById('drop-zone');
    const fileInput = document.getElementById('file-input');

    dropZone.addEventListener('click', () => fileInput.click());

    dropZone.addEventListener('dragover', (event) => {
        event.preventDefault();
        dropZone.style.borderColor = '#333';
    });

    dropZone.addEventListener('dragleave', () => {
        dropZone.style.borderColor = '#bbb';
    });

    dropZone.addEventListener('drop', (event) => {
        event.preventDefault();
        dropZone.style.borderColor = '#bbb';
        const file = event.dataTransfer.files[0];
        const reader = new FileReader();
        reader.onload = () => {
            const csvData = reader.result;
            const base64CSV = btoa(csvData);
            Streamlit.setComponentValue(base64CSV);
        };
        reader.readAsText(file);
    });

    fileInput.addEventListener('change', () => {
        const file = fileInput.files[0];
        const reader = new FileReader();
        reader.onload = () => {
            const csvData = reader.result;
            const base64CSV = btoa(csvData);
            Streamlit.setComponentValue(base64CSV);
        };
        reader.readAsText(file);
    });
    </script>
""", unsafe_allow_html=True)

# Process the uploaded CSV data
csv_data = st.query_params.get("componentValue", [None])[0]
if csv_data:
    csv_bytes = base64.b64decode(csv_data)
    csv_str = csv_bytes.decode("utf-8")
    df = pd.read_csv(pd.compat.StringIO(csv_str))
    st.write("CSV Data Preview:")
    st.write(df.head())

    table_name = st.text_input("Enter Snowflake Table Name")

    if st.button("Load to Snowflake"):
        if table_name:
            load_data_to_snowflake(df, table_name)
        else:
            st.error("Please enter a table name.")
