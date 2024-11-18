import requests
import logging
import time
from bs4 import BeautifulSoup as bs
import json
import re
import pandas as pd
import mysql.connector
from mysql.connector import Error
from utils import path_loader as p

# URL to ping Healthchecks for monitoring task execution
HEALTHCHECKS_URL = "https://hc.example.com/ping/<unique-id>"


def html_to_dataframe(URL_site: str) -> pd.DataFrame:
    """
    Fetches HTML content from a given URL, extracts JavaScript-embedded JSON data,
    and converts it into a pandas DataFrame.

    Args:
        URL_site (str): The URL of the webpage to scrape data from.

    Returns:
        pd.DataFrame: A DataFrame containing the extracted and normalized data.
                      Returns an empty DataFrame if any errors occur.
    """
    try:
        # Fetch the HTML content
        html = requests.get(URL_site, verify=True)
        html.raise_for_status()  # Raise an error for HTTP issues
        soup = bs(html.content, "html.parser")  # Parse the HTML content
        # Extract JSON data from scripts
        raw_json = soup.find_all('script', type='text/javascript')

        # Use regex to isolate the JSON structure
        str_json = "[" + re.search(r'{"i.*":\s*"(.*?)"}',
                                   str(raw_json)).group() + "]"
        str_to_json = json.loads(str_json)  # Parse the JSON string

        # Normalize JSON into a DataFrame
        df = pd.json_normalize(str_to_json)
        return df
    except AttributeError:
        logging.error("Failed to extract JSON from the HTML content.")
        return pd.DataFrame()  # Return an empty DataFrame if JSON extraction fails
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch HTML content: {e}")
        return pd.DataFrame()  # Return an empty DataFrame for HTTP errors


def process_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Processes a DataFrame by renaming columns and removing rows with missing values.

    Args:
        df (pd.DataFrame): The DataFrame to process.

    Returns:
        pd.DataFrame: The processed DataFrame with renamed columns and no missing values.
                      Returns an empty DataFrame if any errors occur.
    """
    new_names = {
        'i': 'ID',
        'n': 'Nombre',
        'a': 'Apellido',
        'd': 'Departamento/Facultad',
        'm': '# de calif.',
        'c': 'Promedio'
    }
    try:
        # Rename columns and drop rows with missing values
        df = df.rename(columns=new_names).dropna()
        return df
    except Exception as e:
        logging.error(f"Error processing data: {e}")
        return pd.DataFrame()


def upload_to_mysql(df: pd.DataFrame, table_name: str, db_config: dict) -> None:
    """
    Uploads a pandas DataFrame to a MySQL database.

    Args:
        df (pd.DataFrame): The DataFrame to upload.
        table_name (str): The name of the table in the database.
        db_config (dict): A dictionary containing MySQL database connection parameters.
                          Expected keys: host, database, user, password.

    Returns:
        None
    """
    try:
        # Establish database connection
        connection = mysql.connector.connect(
            host=db_config["host"],
            database=db_config["database"],
            user=db_config["user"],
            password=db_config["password"],
        )

        if connection.is_connected():
            logging.info("Connected to MySQL database.")

            # Create a cursor and insert data
            cursor = connection.cursor()

            # Convert the DataFrame to a list of tuples for insertion
            data = [tuple(row) for row in df.to_numpy()]
            columns = ", ".join(df.columns)

            # Create a parameterized SQL query
            query = f"INSERT INTO {table_name} ({columns}) VALUES ({', '.join(['%s'] * len(df.columns))})"

            # Execute the query for all rows in the DataFrame
            cursor.executemany(query, data)
            connection.commit()

            logging.info(f"Inserted {cursor.rowcount} rows into {table_name}.")

    except Error as e:
        logging.error(f"Error while connecting to MySQL: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            logging.info("MySQL connection closed.")


def run_pipeline():
    """
    Executes the data pipeline by fetching, processing, and uploading the data to MySQL.
    """
    # URL for the specific data source
    url = 'https://www.misprofesores.com/escuelas/UANL-FCFM_2263'

    # MySQL database configuration
    db_config = {
        "host": "localhost",
        "database": "mis_profesores",
        "user": "root",
        "password": "password",
    }
    table_name = "fcfm_profesores"

    # Fetch and process data
    df = html_to_dataframe(url)
    if not df.empty:
        df_processed = process_data(df)
        logging.info(f"Processed data:\n{df_processed}")

        # Upload to MySQL
        upload_to_mysql(df_processed, table_name, db_config)
    else:
        logging.warning("No data to process.")


def execute_script(healthchecks_url: str, loggin_path: str) -> None:
    """
    Executes the entire script, including logging and Healthchecks integration.

    Args:
        healthchecks_url (str): The Healthchecks ping URL to monitor the script.
        loggin_path (str): The file path for storing log outputs.
    """
    # Configure logging
    logging.basicConfig(
        filename=loggin_path,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    try:
        logging.info("Starting the cron job task...")

        # Run the pipeline
        run_pipeline()

        # Notify Healthchecks of success
        response = requests.get(healthchecks_url)
        response.raise_for_status()  # Raise error if Healthchecks ping fails
        logging.info("Healthchecks ping successful!")

    except Exception as e:
        # Notify Healthchecks of failure
        error_url = f"{healthchecks_url}/fail"
        requests.get(error_url)
        logging.error(f"Healthchecks ping failed: {e}")


def main():
    """
    Entry point of the script. Sets up paths and executes the script.
    """
    # Generate a log file path dynamically using a utility function
    loggin_path = p.generate_file_path('mp_pipeline', 'log')
    execute_script(HEALTHCHECKS_URL, loggin_path)


if __name__ == '__main__':
    main()
