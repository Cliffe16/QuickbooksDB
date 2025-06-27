#Import libraries
from quickbooks.client import QuickBooks
from quickbooks import objects
from configparser import ConfigParser
import pandas as pd
import logging

def get_qb_client(company_config):
    """Initializes and returns a Quickbooks client from company-specifc config."""
    config = ConfigParser
    config.read('config/config.ini')
    qb_creds = config['QUICKBOOKS']
    
    return QuickBooks(
       client_id = qb_creds['client_id'],
       client_secret = qb_creds['client_secret'],
       refresh_token = company_config['QBFilePath'],
       realm_id = company_config['CompanyID'],
       environment = qb_creds['environment']
    )
    
def _flatten_address(addr, prefix):
    """Helper function to flatten address objects to a dictionary"""
    if not addr: #Check first if addr is empty then it returns an empty dictionary
        return {
            f"{prefix}_Line1": None, 
            f"{prefix}_City": None, 
            f"{prefix}_State": None, 
            f"{prefix}_PostalCode": None
            }
    return{
        f"{prefix}_Line1": addr.get('Line1'),
        f"{prefix}_City": addr.get('City'),
        f"{prefix}_State": addr.get('CountrySubDivisionCode'),
        f"{prefix}_PostalCode":addr.get('PostalCode')
    }

def fetch_data(qb_client, qb_object, company_id, transform_func):
    """Generic data fetching function for simple list objects from Quickbooks"""
    try:
        logging.info(f"Fetching {qb_object.__name__}...")
        all_entities = qb_object.all(qb=qb_client) #API call to pull all records specified
        if not all_entities:
            logging.info(f"No records found for {qb_object.__name__}.")
            return pd.DataFrame()
        
        records = [transform_func(e, company_id) for e in all_entities] #Loop through every item in all_entities and 
                                                                        #pass through the transformer function
        
        df = pd.DataFrame(records)
        logging.info(f"Successfully fetched and transformed {len(df)} records for {qb_object.__name__}.")
        return df
    except Exception as e:
        logging.error(f"Failed to fetch {qb_object.__name__}: {e}")
        return pd.DataFrame
    
