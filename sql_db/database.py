import pyodbc
import pandas as pd
from configparser import ConfigParser
import logging

def get_db_connection():
    """Establishes and returns a pyodbc database connection."""
    try:
        config = ConfigParser
        config.read('config/config.ini') #read config.ini
        db_config = config['SQL Server'] #read SQL Server configuration settings
        
        #Define the connection string
        conn_str = (
            f"DRIVER={{{db_config['driver']}}};"  
            f"SERVER={db_config['server']};"
            f"DATABASE={db_config['database']};"
            f"UID={db_config['username']};"
            f"PWD={db_config['password']};"
            "Encrypt=yes;TrustServerCertificate=no;Connection Timeout = 30;"
        )
        return pyodbc.connect(conn_str)
    except Exception as e:
        logging.error(f"Database connection failed: {e}")
        raise

def get_active_companies():
    """Fetches active companies from the control table"""
    logging.info("Fetching active companies from etl.Companies control table.")
    conn = None
    try:
        query = "SELECT CompanyID, CompanyName FROM etl.Companies where IsActive = 1" 
        conn = get_db_connection
        df = pd.read_sql(query, conn)
        return df.to_dict('records')
    finally:
        if conn:
            conn.close()
    

def update_sync_status(company_id, status, message):
    """Updates a sync status for a company in the control table"""
    logging.info(f"Updating sync status for CompanyID {company_id}: {status}")
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        query = """
            UPDATE etl.Companies
            SET LastSyncStatus = ?, LastSyncMessage = ?, LastSyncTimeUTC = GETUTCDATE()
            WHERE CompanyID = ?
        """
        cursor.execute(query, company_id, status, message)
        conn.commit()
    finally:
        if conn:
            conn.close()
    
def upsert_data(df, table_name):
    """Performs an upsert(update existing, insert new operation.
    A temporary table and MERGE statement is used for high efficiency)"""
    #Create a temporary table with the same structure as the tables to be upserted
    temp_table_name = f"#{table_name}_temp" # create temporary  table
    
    conn = get_db_connection
    cursor = conn.cursor()
    cursor.execute(f"SELECT TOP 0 * INTO {temp_table_name} FROM {table_name}") #fetch table structure
    
    #Create a list of tuples from the dataframe to insert into the temp table
    data_tuples = [tuple(x) for x in df.to_numpy()]
    
    #Insert values(placeholders) into the table
    placeholders = ", ".join("?" * len(df.columns)) #placeholders should match the number of columns in the dataframe
    insert_sql = f"INSERT INTO {temp_table_name} VALUES {placeholders}"
    
    cursor.executemany(insert_sql, data_tuples)
    
    #Use a MERGE statement to upsert from the temp table to the main table assuming 'ListID' and 'CompanyID' are composite primary keys
    merge_sql = f"""
        MERGE {table_name} AS target
        USING {temp_table_name} AS source
        ON target.ListID = source.ListID AND target.CompanyID = source.CompanyID
        WHEN MATCHED THEN
            UPDATE
            SET {', '.join([f'target.[{col}] =  source.[{col}]' for col in df.columns])} --updates all columns in the dataframe i.e target table with the lastest data from the source table
        WHEN NOT MATCHED BY TARGET THEN
            INSERT ({', '.join([f'[{col}]' for col in df.columns])}) --list of columns from the target table that have new records to be inserted
            VALUES ({', '.join([f'source.[{col}]' for col in df.columns])}) --list of values to be pulled from the source table as new records
        """
    cursor.execute(merge_sql)
    conn.commit()
    
    #Clean up
    cursor.execute(f"DROP TABLE {temp_table_name}")
    cursor.close()
    conn.close()
            
    
    
    
    
    
    