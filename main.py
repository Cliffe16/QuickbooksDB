# In main.py
import logging
from sql_db import database
from qb_api import client
from quickbooks import objects
import pandas as pd

logging.basicConfig(filename='logs/service.log',level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def sync_list_data(qb_client, company_id, qb_object, transform_func, table_name, pk_cols):
    """Generic sync function for simple list objects. Returns True on success, False on failure."""
    logging.info(f"---Syncing {qb_object.__name__}---")
    try:
        df = client.fetch_data(qb_client, qb_object, company_id, transform_func)
        if df is not None and not df.empty:
            database.upsert_data(df, table_name, pk_cols)
        return True # Return True if fetch and upsert are successful
    except Exception as e:
        logging.error(f"Failed to sync {qb_object.__name__}: {e}")
        return False # Return False on any exception

def sync_transactional_data(qb_client, company_id, qb_object, transform_func, header_table, line_table, header_pk, line_pk):
    """Generic sync function for transactional data. Returns True on success, False on failure."""
    logging.info(f"---Syncing {qb_object.__name__}---")
    try:
        all_transactions = qb_object.all(qb=qb_client)
        if not all_transactions:
            logging.info(f"No records found for {qb_object.__name__}.")
            return True # No records is a success condition

        all_headers, all_lines = [], []
        for txn in all_transactions:
            header, lines = transform_func(txn, company_id)
            all_headers.append(header)
            all_lines.extend(lines)
            
        headers_df = pd.DataFrame(all_headers)
        lines_df = pd.DataFrame(all_lines)
        
        if not headers_df.empty:
            database.upsert_data(headers_df, header_table, header_pk)
        if not lines_df.empty:
            database.upsert_data(lines_df, line_table, line_pk)
        return True # Return True on success
    except Exception as e:
        logging.error(f"Failed to sync {qb_object.__name__}: {e}")
        return False # Return False on any exception

def run_sync_for_company(company_config):
    """Runs the full data synchronization process for a single company."""
    company_id = company_config['CompanyID']
    logging.info(f"===Starting sync for: {company_config['CompanyName']} (ID: {company_id})===")
    
    sync_failed = False  #<-- Initialize a failure flag

    try:
        # Capture both client and new token ---
        qb_client, new_refresh_token = client.get_qb_client(company_config)

        # Update the refresh token in the database 
        if new_refresh_token != company_config['RefreshToken']:
            database.update_refresh_token(company_id, new_refresh_token)
            logging.info(f"Updated refresh token in database for Company ID: {company_id}")
                
        list_entities_to_sync = [
            (objects.Account, client.transform_account, "qb_data.Accounts", ["ListID", "CompanyID"]),
            (objects.Customer, client.transform_customer, "qb_data.Customers", ["ListID", "CompanyID"]),
            (objects.Vendor, client.transform_vendor, "qb_data.Vendors", ["ListID", "CompanyID"]),
            (objects.Employee, client.transform_employee, "qb_data.Employees", ["ListID", "CompanyID"]),
            (objects.Item, client.transform_item, "qb_data.Items", ["ListID", "CompanyID"]),
            (objects.Class, client.transform_class, "qb_data.Classes", ["ListID", "CompanyID"]),
            (objects.Payment, client.transform_receive_payment, "qb_data.ReceivePayments", ["TxnID", "CompanyID"]),
            (objects.BillPayment, client.transform_bill_payment, "qb_data.BillPayments", ["TxnID", "CompanyID"]),
        ]
        
        for qb_obj, transform, table, pk in list_entities_to_sync:
            # If a sync function returns False, set the failure flag
            if not sync_list_data(qb_client, company_id, qb_obj, transform, table, pk):
                sync_failed = True
            
        transactional_entities_to_sync = [
            (objects.Invoice, client.transform_invoice, "qb_data.Invoices", "qb_data.InvoiceLines", ["TxnID", "CompanyID"], ["TxnLineID", "CompanyID"]),
            (objects.Bill, client.transform_bill, "qb_data.Bills", "qb_data.BillLines", ["TxnID", "CompanyID"], ["TxnLineID", "CompanyID"]),
            (objects.JournalEntry, client.transform_journal_entry, "qb_data.JournalEntries", "qb_data.JournalEntryLines", ["TxnID", "CompanyID"], ["TxnLineID", "CompanyID"]),
            (objects.SalesReceipt, client.transform_sales_order, "qb_data.SalesOrders", "qb_data.SalesOrderLines", ["TxnID", "CompanyID"], ["TxnLineID", "CompanyID"]),
            (objects.PurchaseOrder, client.transform_purchase_order, "qb_data.PurchaseOrders", "qb_data.PurchaseOrderLines", ["TxnID", "CompanyID"], ["TxnLineID", "CompanyID"]),
        ]

        for qb_obj, transform, h_tbl, l_tbl, h_pk, l_pk in transactional_entities_to_sync:
            # If a sync function returns False, set the failure flag
            if not sync_transactional_data(qb_client, company_id, qb_obj, transform, h_tbl, l_tbl, h_pk, l_pk):
                sync_failed = True

        # Now, check the flag to determine the final status
        if sync_failed:
            error_message = 'One or more entities failed to sync. Check logs for details.'
            database.update_sync_status(company_id, 'Failed', error_message)
            logging.error(f"===Sync failed for: {company_config['CompanyName']}===")
        else:
            database.update_sync_status(company_id, 'Success', 'Full sync completed successfully.')
            logging.info(f"===Sync successful for: {company_config['CompanyName']}===")
    
    except Exception as e:
        error_message = f"A critical error occurred during sync for {company_id}: {e}"
        logging.error(error_message)
        database.update_sync_status(company_id, 'Failed', error_message)
        
if __name__ == "__main__":
    logging.info("ETL Process Started.")
    active_companies = database.get_active_companies()
    
    if not active_companies:
        logging.warning("No active companies found in etl.Companies to process.")
    else:
        for company in active_companies:
            run_sync_for_company(company)
            
    logging.info("ETL Process finished.")