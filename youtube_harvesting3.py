import streamlit as st
import mysql.connector
import pandas as pd

# Set up MySQL connection
mysql_connection = mysql.connector.connect(
    host="<mysql_host>",
    user="<mysql_user>",
    password="<mysql_password>",
    database="<mysql_database>"
)
mysql_cursor = mysql_connection.cursor()

# Function to execute SQL query and display results as table
def execute_query(query):
    mysql_cursor.execute(query)
    result = mysql_cursor.fetchall()
    columns = [column[0] for column in mysql_cursor.description]
    df = pd.DataFrame(result, columns=columns)
    st.table(df)

# Streamlit app
def main():
    st.title("YouTube Data Analysis")

    query = st.text_input("Enter SQL query")

    if st.button("Execute Query"):
        execute_query(query)

# Run the Streamlit app
if __name__ == "__main__":
    main()
