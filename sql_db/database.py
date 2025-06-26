import pyodbc
import pandas as pd
from configparser import ConfigParser

def get_db_connection():
    """Establishes and returns a pyodbc database connection."""
    config = ConfigParser
    config.read('config/config.ini') #read config.ini
    db_config = config['SQL Server'] #read SQL Server configuration settings
    
    #Define the connection string
    conn_str = (
        f"DRIVER={db_config['driver']};"
        f"SERVER={db_config['server']};"
        f"DATABASE={db_config['database']};"
        f"UID={db_config['uid']};"
        f"PWD={db_config['pwd']}"
        "Encrypt=yes;TrustedServerCertificate=no;Connection Timeout = 30;"
    )
    return pyodbc.connect(conn_str)

