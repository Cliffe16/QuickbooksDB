import pyodbc
import pandas as pd
from configparser import ConfigParser
import logging

def get_db_connection():
    """Establishes and returns a pyodbc database connection."""
    try:
        config = ConfigParser()
        config.read('config/config.ini') #read config.ini
        db_config = config['SQL_SERVER'] #read SQL Server configuration settings
        
        #Define the connection string
        conn_str = (
            f"DRIVER={{{db_config['driver']}}};"  
            f"SERVER={db_config['server']};"
            f"DATABASE={db_config['database']};"
            f"UID={db_config['username']};"
            f"PWD={db_config['password']};"
            "Encrypt=yes;TrustServerCertificate=yes;Connection Timeout = 30;"
        )
        return pyodbc.connect(conn_str)
    except Exception as e:
        logging.error(f"Database connection failed: {e}")
        raise

def get_active_companies():
    """Fetches active companies from the control table"""
    logging.info("Fetching active companies from etl.Companies control table.")
    conn_str = None
    try:
        query = "SELECT CompanyID, CompanyName FROM etl.Companies where IsActive = 1" 
        conn_str = get_db_connection()
        df = pd.read_sql(query, conn_str)
        return df.to_dict('records')
    finally:
        if conn_str:
            conn_str.close()
    

def update_sync_status(company_id, status, message):
    """Updates a sync status for a company in the control table"""
    logging.info(f"Updating sync status for CompanyID {company_id}: {status}")
    conn_str = None
    try:
        conn_str = get_db_connection()
        cursor = conn_str.cursor()
        query = """
            UPDATE etl.Companies
            SET LastSyncStatus = ?, LastSyncMessage = ?, LastSyncTimeUTC = GETUTCDATE()
            WHERE CompanyID = ?
        """
        cursor.execute(query, status, message, company_id)
        conn_str.commit()
    finally:
        if conn_str:
            conn_str.close()
    
def upsert_data(df, table_name, pk_cols):
    """Performs an upsert(update existing, insert new operation.
    A temporary table and MERGE statement is used for high efficiency)"""
    if df.empty:
        logging.info(f"DataFrame for {table_name} is empty. Nothing to upsert.")
        return

    logging.info(f"Starting upsert process for {len(df)} rows into {table_name}.")
    conn_str = None
    
    #Create a temporary table with the same structure as the tables to be upserted
    try:
        conn_str = get_db_connection()
        cursor = conn_str.cursor()
        
        # Sanitize column names for SQL
        df.columns = [f"[{col}]" for col in df.columns]
            
        # Create the ON clause for the MERGE statement
        on_clause = " AND ".join([f"target.[{pk}] = source.[{pk}]" for pk in pk_cols])
            
        # Create the UPDATE SET clause
        update_cols = [col for col in df.columns if f"[{col}]" not in pk_cols]
        update_clause = ", ".join([f"target.{col} = source.{col}" for col in update_cols])

        
        #Create the INSERT clause
        all_cols = ", ".join(df.columns)
        source_cols = ", ".join([f"source.{col}" for col in df.columns])
        
        # Create temporary  table
        temp_table_name = f"##{table_name.replace('.','_')}_temp" 
        cursor.execute(f"SELECT TOP 0 * INTO {temp_table_name} FROM {table_name}") #fetch table structure
        
        #Create a list of tuples from the dataframe to insert into the temp table
        data_tuples = [tuple(x) for x in df.to_numpy()]
        
        #Insert values(placeholders) into the table
        placeholders = ", ".join("?" * len(df.columns)) #placeholders should match the number of columns in the dataframe
        insert_sql = f"""INSERT INTO {temp_table_name} ({all_cols}) 
                        VALUES ({placeholders})"""
        cursor.fast_executemany = True
        cursor.executemany(insert_sql, data_tuples)
        
        #Use a MERGE statement to upsert from the temp table to the main table 
        if update_clause:
                merge_sql = f"""
                    MERGE {table_name} AS target
                    USING {temp_table_name} AS source
                    ON {on_clause}
                    WHEN MATCHED THEN
                        UPDATE SET {update_clause}
                    WHEN NOT MATCHED BY TARGET THEN
                        INSERT ({all_cols}) VALUES ({source_cols});
                """
        
        else:
            merge_sql = f"""
                MERGE {table_name} AS target
                USING {temp_table_name} AS source
                ON {on_clause}
                WHEN NOT MATCHED BY TARGET THEN
                    INSERT ({all_cols}) 
                    VALUES ({source_cols}) 
                """
        cursor.execute(merge_sql)
        conn_str.commit()
        logging.info(f"Upsert for {table_name} successful. {cursor.rowcount} rows affected.")
        cursor.execute(f"DROP TABLE {temp_table_name}")
    
    except Exception as e:
        logging.error(f"An error occurred during upsert to {table_name}: {e}")
        if conn_str:
            conn_str.rollback()
        raise
    finally:
        if conn_str:
            conn_str.close()
            
    
    
    
    
    
    