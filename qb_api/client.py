#Import libraries
from quickbooks.client import QuickBooks
from intuitlib.client import AuthClient
from sql_db import database
from quickbooks import objects
from configparser import ConfigParser 
import pandas as pd
import logging
from decimal import Decimal
import numpy as np

def get_qb_client(company_config):
    """Initializes and returns a Quickbooks client with hardcoded credentials for a definitive test."""
    logging.info("--- Initializing QuickBooks client from configuration ---")

    # Load application credentials from config.ini 
    config = ConfigParser()
    config.read('config/config.ini') # Assumes config.ini is in the root directory
    qb_config = config['QUICKBOOKS']

    # Load company-specific credentials from the company_config parameter 
    refresh_token = company_config['RefreshToken']
    company_id = company_config['CompanyID']

    auth_client = AuthClient(
        client_id=qb_config.get('client_id'),
        client_secret=qb_config.get('client_secret'),
        environment=qb_config.get('environment'),
        redirect_uri='https://developer.intuit.com/v2/OAuth2Playground/RedirectUrl'
    )

    auth_client.refresh_token = refresh_token
    try:
        auth_client.refresh()
        logging.info(f"Token refresh SUCCEEDED for Company ID: {company_id}.")
    except Exception as e:
        logging.error(f"Token refresh FAILED for Company ID: {company_id}. Error: {e}")
        raise e

    # Create the QuickBooks client instance
    qb_client = QuickBooks(
        auth_client=auth_client,
        company_id=company_id
    )
    # Return the client and the new refresh token to be saved
    return qb_client, auth_client.refresh_token

def _flatten_address(addr, prefix):
    """Helper function to flatten address objects to a dictionary"""
    if not addr:
        return {f"{prefix}_Line1": None, f"{prefix}_City": None, f"{prefix}_State": None, f"{prefix}_PostalCode": None}
    # Correctly use getattr on the address object itself
    return {
        f"{prefix}_Line1": getattr(addr, 'Line1', None),
        f"{prefix}_City": getattr(addr, 'City', None),
        f"{prefix}_State": getattr(addr, 'CountrySubDivisionCode', None),
        f"{prefix}_PostalCode": getattr(addr, 'PostalCode', None)
    }

# --- Helper functions to sanitize data ---
def to_date(date_val):
    """Converts a value to a YYYY-MM-DD date string or None."""
    if not date_val:
        return None
    return str(date_val)[:10]

def to_numeric(value):
    """Converts empty strings to None for numeric fields, otherwise returns the value."""
    if value == '':
        return None
    return value

# --- FIX: New helper function to explicitly cast to float for FLOAT columns ---
def to_float(value):
    """Safely converts a value to a float for FLOAT columns, or returns None."""
    if value is None or value == '':
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

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
        
        # FINAL FIX: Replace numpy's Not a Number (NaN) with None.
        # The pyodbc driver cannot handle NaN values, but it can handle None (for NULL).
        df = df.replace({np.nan: None})
        
        logging.info(f"Successfully fetched and transformed {len(df)} records for {qb_object.__name__}.")
        return df
    except Exception as e:
        logging.error(f"Failed to fetch {qb_object.__name__}: {e}")
        return pd.DataFrame()

#Transformation Functions for each Entity
def transform_account(a, cid):
    meta_data = getattr(a, 'MetaData', {})
    return {
        "ListID": a.Id, "CompanyID": cid, "Name": a.Name, "FullName": a.FullyQualifiedName,
        "IsActive": int(a.Active or 0), "ParentRef_ListID": getattr(a.ParentRef, 'value', None),
        "Sublevel": int(a.SubAccount or 0), "AccountType": a.AccountType, "DetailType": a.AccountSubType,
        "AccountNumber": a.AcctNum, "BankNumber": None, "RoutingNumber": getattr(a, 'RoutingNum', None),
        "Description": a.Description, "Balance": to_numeric(a.CurrentBalance),
        "TotalBalance": to_numeric(a.CurrentBalanceWithSubAccounts),
        "LastModifiedUTC": to_date(getattr(meta_data, 'LastUpdatedTime', None)),
        "DateCreatedUTC": to_date(getattr(meta_data, 'CreateTime', None))
    }

def transform_customer(c, cid):
    meta_data = getattr(c, 'MetaData', {})
    job_info = getattr(c, 'JobInfo', None)
    record = {
        "ListID": c.Id, "CompanyID": cid, "FullName": c.DisplayName, "CompanyName": c.CompanyName,
        "FirstName": c.GivenName, "MiddleName": c.MiddleName, "LastName": c.FamilyName,
        "IsActive": int(c.Active or 0), "ParentRef_ListID": getattr(c.ParentRef, 'value', None),
        "Sublevel": int(c.Job or 0), "Email": getattr(c.PrimaryEmailAddr, 'Address', None),
        "Phone": getattr(c.PrimaryPhone, 'FreeFormNumber', None),
        "AltPhone": getattr(c.AlternatePhone, 'FreeFormNumber', None),
        "Fax": getattr(c.Fax, 'FreeFormNumber', None), "Balance": to_numeric(c.Balance),
        "TotalBalance": to_numeric(c.BalanceWithJobs), "CreditLimit": None,
        "JobStatus": getattr(job_info, 'JobStatus', None),
        "LastModifiedUTC": to_date(getattr(meta_data, 'LastUpdatedTime', None)),
        "DateCreatedUTC": to_date(getattr(meta_data, 'CreateTime', None))
    }
    record.update(_flatten_address(getattr(c, 'BillAddr', None), "BillAddress"))
    record.update(_flatten_address(getattr(c, 'ShipAddr', None), "ShipAddress"))
    return record

def transform_vendor(v, cid):
    meta_data = getattr(v, 'MetaData', {})
    return {
        "ListID": v.Id, "CompanyID": cid, "Name": v.DisplayName, "CompanyName": v.CompanyName,
        "FirstName": v.GivenName, "LastName": v.FamilyName, "IsActive": int(v.Active or 0),
        "Email": getattr(v.PrimaryEmailAddr, 'Address', None),
        "Phone": getattr(v.PrimaryPhone, 'FreeFormNumber', None), "Balance": to_numeric(v.Balance),
        "VendorTaxIdent": v.TaxIdentifier, "Is1099Eligible": int(v.Vendor1099 or 0),
        "LastModifiedUTC": to_date(getattr(meta_data, 'LastUpdatedTime', None)),
        "DateCreatedUTC": to_date(getattr(meta_data, 'CreateTime', None))
    }

def transform_employee(e, cid):
    meta_data = getattr(e, 'MetaData', {})
    return {
        "ListID": e.Id, "CompanyID": cid, "FirstName": e.GivenName, "MiddleName": e.MiddleName,
        "LastName": e.FamilyName, "FullName": e.DisplayName, "IsActive": int(e.Active or 0),
        "EmployeeType": getattr(e, 'EmployeeType', None), "Gender": getattr(e, 'Gender', None),
        "HiredDate": to_date(getattr(e, 'HiredDate', None)),
        "ReleasedDate": to_date(getattr(e, 'ReleasedDate', None)),
        "BirthDate": to_date(getattr(e, 'BirthDate', None)),
        "LastModifiedUTC": to_date(getattr(meta_data, 'LastUpdatedTime', None)),
        "DateCreatedUTC": to_date(getattr(meta_data, 'CreateTime', None))
    }

def transform_item(i, cid):
    meta_data = getattr(i, 'MetaData', {})
    return {
        "ListID": i.Id, "CompanyID": cid, "FullName": i.FullyQualifiedName, "IsActive": int(i.Active or 0),
        "ItemType": i.Type, "SalesDesc": i.Description,
        "SalesPrice": to_numeric(i.UnitPrice),
        "PurchaseDesc": i.PurchaseDesc,
        "PurchaseCost": to_numeric(i.PurchaseCost),
        # FIX: Use to_float for FLOAT columns to ensure correct data type.
        "QuantityOnHand": to_float(i.QtyOnHand),
        "QuantityOnOrder": to_float(getattr(i, 'QtyOnPurchaseOrder', None)),
        "QuantityOnSalesOrder": to_float(getattr(i, 'QtyOnSalesOrder', None)),
        "AverageCost": to_numeric(getattr(i, 'AvgCost', None)),
        "ParentRef_ListID": getattr(i.ParentRef, 'value', None),
        "IncomeAccountRef_FullName": getattr(i.IncomeAccountRef, 'name', None),
        "ExpenseAccountRef_FullName": getattr(i.ExpenseAccountRef, 'name', None),
        "AssetAccountRef_FullName": getattr(i.AssetAccountRef, 'name', None),
        "LastModifiedUTC": to_date(getattr(meta_data, 'LastUpdatedTime', None)),
        "DateCreatedUTC": to_date(getattr(meta_data, 'CreateTime', None))
    }

def transform_class(c, cid):
    meta_data = getattr(c, 'MetaData', {})
    return {
        "ListID": c.Id, "CompanyID": cid, "FullName": c.FullyQualifiedName, "IsActive": int(c.Active or 0),
        "ParentRef_ListID": getattr(c.ParentRef, 'value', None),
        "LastModifiedUTC": to_date(getattr(meta_data, 'LastUpdatedTime', None)),
        "DateCreatedUTC": to_date(getattr(meta_data, 'CreateTime', None))
    }

def transform_invoice(inv, cid):
    meta_data = getattr(inv, 'MetaData', {})
    txn_tax_detail = getattr(inv, 'TxnTaxDetail', {})

    # FIX: Explicitly cast all values to Decimal for calculations to prevent TypeError.
    tax_val = to_numeric(getattr(txn_tax_detail, 'TotalTax', 0))
    total_val = to_numeric(inv.TotalAmt)
    balance_val = to_numeric(inv.Balance)
    
    tax_total = Decimal(str(tax_val or '0'))
    total_amt = Decimal(str(total_val or '0'))
    balance = Decimal(str(balance_val or '0'))

    header = {
        "TxnID": inv.Id, "CompanyID": cid,
        "CustomerRef_ListID": getattr(inv.CustomerRef, 'value', None),
        "CustomerRef_FullName": getattr(inv.CustomerRef, 'name', None),
        "ClassRef_FullName": getattr(getattr(inv, 'ClassRef', None), 'name', None),
        "TxnDate": to_date(inv.TxnDate),
        "RefNumber": inv.DocNumber,
        "DueDate": to_date(inv.DueDate),
        "ShipDate": to_date(inv.ShipDate),
        "Subtotal": total_amt - tax_total,
        "SalesTaxTotal": tax_total,
        "AppliedAmount": balance,
        "BalanceRemaining": balance,
        "TotalAmount": total_amt,
        "IsPaid": int(balance == Decimal('0')),
        "IsPending": int(inv.EmailStatus == 'NotSet'), "Memo": inv.PrivateNote,
        "LastModifiedUTC": to_date(getattr(meta_data, 'LastUpdatedTime', None)),
        "DateCreatedUTC": to_date(getattr(meta_data, 'CreateTime', None))
    }
    lines = []
    if inv.Line:
        for line in inv.Line:
            if getattr(line, 'SalesItemLineDetail', None):
                detail = line.SalesItemLineDetail
                lines.append({
                    "TxnLineID": f"{inv.Id}-{line.Id}",
                    "Parent_TxnID": inv.Id, "CompanyID": cid,
                    "ItemRef_ListID": getattr(detail.ItemRef, 'value', None),
                    "ItemRef_FullName": getattr(detail.ItemRef, 'name', None),
                    "Description": line.Description,
                    "Quantity": to_numeric(getattr(detail, 'Qty', None)),
                    "Rate": to_numeric(getattr(detail, 'UnitPrice', None)),
                    "Amount": to_numeric(line.Amount),
                    "ClassRef_FullName": getattr(getattr(detail, 'ClassRef', None), 'name', None),
                    "SalesTaxCodeRef_FullName": getattr(getattr(detail, 'TaxCodeRef', None), 'name', None),
                })
    return header, lines

def transform_bill(bill, cid):
    meta_data = getattr(bill, 'MetaData', {})
    balance_val = to_numeric(bill.Balance)
    balance = Decimal(str(balance_val or '0'))
    header = {
        "TxnID": bill.Id, "CompanyID": cid,
        "VendorRef_ListID": getattr(bill.VendorRef, 'value', None),
        "VendorRef_FullName": getattr(bill.VendorRef, 'name', None),
        "TxnDate": to_date(bill.TxnDate),
        "DueDate": to_date(bill.DueDate),
        "RefNumber": bill.DocNumber,
        "AmountDue": to_numeric(bill.TotalAmt),
        "IsPaid": int(balance == Decimal('0')),
        "Memo": bill.PrivateNote,
        "LastModifiedUTC": to_date(getattr(meta_data, 'LastUpdatedTime', None)),
        "DateCreatedUTC": to_date(getattr(meta_data, 'CreateTime', None))
    }
    lines = []
    if bill.Line:
        for line in bill.Line:
            if getattr(line, 'ItemBasedExpenseLineDetail', None):
                detail = line.ItemBasedExpenseLineDetail
                lines.append({
                    "TxnLineID": f"{bill.Id}-{line.Id}",
                    "Parent_TxnID": bill.Id, "CompanyID": cid,
                    "ItemRef_ListID": getattr(detail.ItemRef, 'value', None),
                    "ItemRef_FullName": getattr(detail.ItemRef, 'name', None),
                    "Description": line.Description,
                    "Quantity": to_numeric(getattr(detail, 'Qty', None)),
                    "Cost": to_numeric(getattr(detail, 'UnitPrice', None)),
                    "Amount": to_numeric(line.Amount),
                    "ClassRef_FullName": getattr(getattr(detail, 'ClassRef', None), 'name', None),
                    "CustomerRef_FullName": getattr(getattr(detail, 'CustomerRef', None), 'name', None),
                })
    return header, lines

def transform_journal_entry(je, cid):
    meta_data = getattr(je, 'MetaData', {})
    header = {
        "TxnID": je.Id, "CompanyID": cid,
        "TxnDate": to_date(je.TxnDate),
        "RefNumber": je.DocNumber,
        "IsAdjustment": int(je.Adjustment or 0),
        "Memo": je.PrivateNote,
        "LastModifiedUTC": to_date(getattr(meta_data, 'LastUpdatedTime', None)),
        "DateCreatedUTC": to_date(getattr(meta_data, 'CreateTime', None))
    }
    lines = []
    if je.Line:
        for line in je.Line:
            if getattr(line, 'JournalEntryLineDetail', None):
                detail = line.JournalEntryLineDetail
                lines.append({
                    "TxnLineID": f"{je.Id}-{line.Id}",
                    "Parent_TxnID": je.Id, "CompanyID": cid,
                    "JournalLineType": getattr(detail, 'PostingType', None),
                    "Amount": to_numeric(line.Amount),
                    "AccountRef_FullName": getattr(detail.AccountRef, 'name', None),
                    "EntityRef_FullName": getattr(getattr(getattr(detail, 'Entity', None), 'EntityRef', None), 'name', None),
                    "ClassRef_FullName": getattr(getattr(detail, 'ClassRef', None), 'name', None),
                    "Memo": line.Description,
                })
    return header, lines

def transform_receive_payment(p, cid):
    meta_data = getattr(p, 'MetaData', {})
    return {
        "TxnID": p.Id, "CompanyID": cid,
        "CustomerRef_FullName": getattr(p.CustomerRef, 'name', None),
        "TxnDate": to_date(p.TxnDate),
        "RefNumber": p.PaymentRefNum,
        "TotalAmount": to_numeric(p.TotalAmt),
        "PaymentMethodRef_FullName": getattr(p.PaymentMethodRef, 'name', None),
        "Memo": p.PrivateNote,
        "UnusedPayment": to_numeric(p.UnappliedAmt),
        "LastModifiedUTC": to_date(getattr(meta_data, 'LastUpdatedTime', None)),
        "DateCreatedUTC": to_date(getattr(meta_data, 'CreateTime', None))
    }

def transform_bill_payment(bp, cid):
    meta_data = getattr(bp, 'MetaData', {})
    bank_account = None
    if getattr(bp, 'PayType', None) == 'Check' and getattr(bp, 'CheckPayment', None):
        bank_account = getattr(bp.CheckPayment, 'BankAccountRef', None)
    elif getattr(bp, 'PayType', None) == 'CreditCard' and getattr(bp, 'CreditCardPayment', None):
        bank_account = getattr(bp.CreditCardPayment, 'CreditCardAccountRef', None)

    return {
        "TxnID": bp.Id, "CompanyID": cid,
        "VendorRef_FullName": getattr(bp.VendorRef, 'name', None),
        "TxnDate": to_date(bp.TxnDate),
        "RefNumber": bp.DocNumber,
        "Amount": to_numeric(bp.TotalAmt),
        "PayType": bp.PayType,
        "BankAccountRef_FullName": getattr(bank_account, 'name', None),
        "Memo": bp.PrivateNote,
        "LastModifiedUTC": to_date(getattr(meta_data, 'LastUpdatedTime', None)),
        "DateCreatedUTC": to_date(getattr(meta_data, 'CreateTime', None))
    }

def transform_sales_order(so, cid):
    meta_data = getattr(so, 'MetaData', {})
    header = {
        "TxnID": so.Id, "CompanyID": cid,
        "CustomerRef_FullName": getattr(so.CustomerRef, 'name', None),
        "TxnDate": to_date(so.TxnDate),
        "RefNumber": so.DocNumber,
        "TotalAmount": to_numeric(so.TotalAmt),
        "IsFullyInvoiced": int(getattr(so, 'POStatus', None) == 'FullyInvoiced'),
        "IsManuallyClosed": int(getattr(so, 'ManuallyClosed', False)),
        "Memo": so.PrivateNote,
        "LastModifiedUTC": to_date(getattr(meta_data, 'LastUpdatedTime', None)),
        "DateCreatedUTC": to_date(getattr(meta_data, 'CreateTime', None))
    }
    lines = []
    if so.Line:
        for line in so.Line:
            if getattr(line, 'SalesItemLineDetail', None):
                detail = line.SalesItemLineDetail
                lines.append({
                    "TxnLineID": f"{so.Id}-{line.Id}",
                    "Parent_TxnID": so.Id, "CompanyID": cid,
                    "ItemRef_FullName": getattr(getattr(detail, 'ItemRef', None), 'name', None),
                    "Description": line.Description,
                    "Quantity": to_numeric(getattr(detail, 'Qty', None)),
                    "Rate": to_numeric(getattr(detail, 'UnitPrice', None)),
                    "Amount": to_numeric(line.Amount)
                })
    return header, lines

def transform_purchase_order(po, cid):
    meta_data = getattr(po, 'MetaData', {})
    header = {
        "TxnID": po.Id, "CompanyID": cid,
        "VendorRef_FullName": getattr(po.VendorRef, 'name', None),
        "TxnDate": to_date(po.TxnDate),
        "RefNumber": po.DocNumber,
        "TotalAmount": to_numeric(po.TotalAmt),
        "IsFullyReceived": int(getattr(po, 'POStatus', None) == 'Closed'),
        "IsManuallyClosed": int(getattr(po, 'ManuallyClosed', False)),
        "Memo": po.PrivateNote,
        "LastModifiedUTC": to_date(getattr(meta_data, 'LastUpdatedTime', None)),
        "DateCreatedUTC": to_date(getattr(meta_data, 'CreateTime', None))
    }
    lines = []
    if po.Line:
        for line in po.Line:
            if getattr(line, 'ItemBasedExpenseLineDetail', None):
                detail = line.ItemBasedExpenseLineDetail
                lines.append({
                    "TxnLineID": f"{po.Id}-{line.Id}",
                    "Parent_TxnID": po.Id, "CompanyID": cid,
                    "ItemRef_FullName": getattr(detail.ItemRef, 'name', None),
                    "Description": line.Description,
                    "Quantity": to_numeric(getattr(detail, 'Qty', None)),
                    "Rate": to_numeric(getattr(detail, 'UnitPrice', None)),
                    "Amount": to_numeric(line.Amount)
                })
    return header, lines