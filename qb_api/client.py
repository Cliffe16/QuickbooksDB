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
    
#Transformation Functions for each Entity
def transform_account(a, cid):
    return {
        "ListID": a.Id, "CompanyID": cid, "Name": a.Name, "FullName": a.FullyQualifiedName,
        "IsActive": a.Active, "ParentRef_ListID": a.ParentRef.value if a.ParentRef else None,
        "Sublevel": a.SubAccount, "AccountType": a.AccountType, "DetailType": a.AccountSubType,
        "AccountNumber": a.AcctNum, "BankNumber": None, "RoutingNumber": a.RoutingNum,
        "Description": a.Description, "Balance": a.CurrentBalance,
        "TotalBalance": a.CurrentBalanceWithSubAccounts,
        "LastModifiedUTC": a.MetaData.get('LastUpdatedTime'), "DateCreatedUTC": a.MetaData.get('CreateTime')
    }

def transform_customer(c, cid):
    record = {
        "ListID": c.Id, "CompanyID": cid, "FullName": c.DisplayName, "CompanyName": c.CompanyName,
        "FirstName": c.GivenName, "MiddleName": c.MiddleName, "LastName": c.FamilyName,
        "IsActive": c.Active, "ParentRef_ListID": c.ParentRef.value if c.ParentRef else None,
        "Sublevel": c.Job, "Email": c.PrimaryEmailAddr.Address if c.PrimaryEmailAddr else None,
        "Phone": c.PrimaryPhone.FreeFormNumber if c.PrimaryPhone else None,
        "AltPhone": c.AlternatePhone.FreeFormNumber if c.AlternatePhone else None,
        "Fax": c.Fax.FreeFormNumber if c.Fax else None, "Balance": c.Balance,
        "TotalBalance": c.BalanceWithJobs, "CreditLimit": None, "JobStatus": c.JobInfo.JobStatus if c.JobInfo else None,
        "LastModifiedUTC": c.MetaData.get('LastUpdatedTime'), "DateCreatedUTC": c.MetaData.get('CreateTime')
    }
    record.update(_flatten_address(c.BillAddr, "BillAddress"))
    record.update(_flatten_address(c.ShipAddr, "ShipAddress"))
    return record

def transform_vendor(v, cid):
    return {
        "ListID": v.Id, "CompanyID": cid, "Name": v.DisplayName, "CompanyName": v.CompanyName,
        "FirstName": v.GivenName, "LastName": v.FamilyName, "IsActive": v.Active,
        "Email": v.PrimaryEmailAddr.Address if v.PrimaryEmailAddr else None,
        "Phone": v.PrimaryPhone.FreeFormNumber if v.PrimaryPhone else None, "Balance": v.Balance,
        "VendorTaxIdent": v.TaxIdentifier, "Is1099Eligible": v.Vendor1099,
        "LastModifiedUTC": v.MetaData.get('LastUpdatedTime'), "DateCreatedUTC": v.MetaData.get('CreateTime')
    }

def transform_employee(e, cid):
    return {
        "ListID": e.Id, "CompanyID": cid, "FirstName": e.GivenName, "MiddleName": e.MiddleName,
        "LastName": e.FamilyName, "FullName": e.DisplayName, "IsActive": e.Active,
        "EmployeeType": e.EmployeeType, "Gender": e.Gender, "HiredDate": e.HiredDate,
        "ReleasedDate": e.ReleasedDate, "BirthDate": e.BirthDate,
        "LastModifiedUTC": e.MetaData.get('LastUpdatedTime'), "DateCreatedUTC": e.MetaData.get('CreateTime')
    }

def transform_item(i, cid):
    return {
        "ListID": i.Id, "CompanyID": cid, "FullName": i.FullyQualifiedName, "IsActive": i.Active,
        "ItemType": i.Type, "SalesDesc": i.Description, "SalesPrice": i.UnitPrice,
        "PurchaseDesc": i.PurchaseDesc, "PurchaseCost": i.PurchaseCost, "QuantityOnHand": i.QtyOnHand,
        "QuantityOnOrder": i.QtyOnPurchaseOrder, "QuantityOnSalesOrder": i.QtyOnSalesOrder, "AverageCost": i.AvgCost,
        "ParentRef_ListID": i.ParentRef.value if i.ParentRef else None,
        "IncomeAccountRef_FullName": i.IncomeAccountRef.name if i.IncomeAccountRef else None,
        "ExpenseAccountRef_FullName": i.ExpenseAccountRef.name if i.ExpenseAccountRef else None,
        "AssetAccountRef_FullName": i.AssetAccountRef.name if i.AssetAccountRef else None,
        "LastModifiedUTC": i.MetaData.get('LastUpdatedTime'), "DateCreatedUTC": i.MetaData.get('CreateTime')
    }

def transform_class(c, cid):
    return {
        "ListID": c.Id, "CompanyID": cid, "FullName": c.FullyQualifiedName, "IsActive": c.Active,
        "ParentRef_ListID": c.ParentRef.value if c.ParentRef else None,
        "LastModifiedUTC": c.MetaData.get('LastUpdatedTime'), "DateCreatedUTC": c.MetaData.get('CreateTime')
    }

def transform_invoice(inv, cid):
    header = {
        "TxnID": inv.Id, "CompanyID": cid,
        "CustomerRef_ListID": inv.CustomerRef.value, "CustomerRef_FullName": inv.CustomerRef.name,
        "ClassRef_FullName": inv.ClassRef.name if inv.ClassRef else None, "TxnDate": inv.TxnDate,
        "RefNumber": inv.DocNumber, "DueDate": inv.DueDate, "ShipDate": inv.ShipDate,
        "Subtotal": inv.TotalAmt - inv.TxnTaxDetail.get("TotalTax", 0),
        "SalesTaxTotal": inv.TxnTaxDetail.get("TotalTax", 0), "AppliedAmount": inv.Balance,
        "BalanceRemaining": inv.Balance, "TotalAmount": inv.TotalAmt, "IsPaid": inv.Balance == 0,
        "IsPending": inv.EmailStatus == 'NotSet', "Memo": inv.PrivateNote,
        "LastModifiedUTC": inv.MetaData.get('LastUpdatedTime'), "DateCreatedUTC": inv.MetaData.get('CreateTime')
    }
    lines = []
    for line in inv.Line:
        if line.SalesItemLineDetail:
            lines.append({
                "TxnLineID": line.Id, "Parent_TxnID": inv.Id, "CompanyID": cid,
                "ItemRef_ListID": line.SalesItemLineDetail.ItemRef.value,
                "ItemRef_FullName": line.SalesItemLineDetail.ItemRef.name,
                "Description": line.Description, "Quantity": line.SalesItemLineDetail.get("Qty"),
                "Rate": line.SalesItemLineDetail.get("UnitPrice"), "Amount": line.Amount,
                "ClassRef_FullName": line.SalesItemLineDetail.ClassRef.name if line.SalesItemLineDetail.ClassRef else None,
                "SalesTaxCodeRef_FullName": line.SalesItemLineDetail.TaxCodeRef.name if line.SalesItemLineDetail.TaxCodeRef else None,
            })
    return header, lines

def transform_bill(bill, cid):
    header = {
        "TxnID": bill.Id, "CompanyID": cid,
        "VendorRef_ListID": bill.VendorRef.value, "VendorRef_FullName": bill.VendorRef.name,
        "TxnDate": bill.TxnDate, "DueDate": bill.DueDate, "RefNumber": bill.DocNumber,
        "AmountDue": bill.TotalAmt, "IsPaid": bill.Balance == 0, "Memo": bill.PrivateNote,
        "LastModifiedUTC": bill.MetaData.get('LastUpdatedTime'), "DateCreatedUTC": bill.MetaData.get('CreateTime')
    }
    lines = []
    for line in bill.Line:
        if line.ItemBasedExpenseLineDetail:
            lines.append({
                "TxnLineID": line.Id, "Parent_TxnID": bill.Id, "CompanyID": cid,
                "ItemRef_ListID": line.ItemBasedExpenseLineDetail.ItemRef.value,
                "ItemRef_FullName": line.ItemBasedExpenseLineDetail.ItemRef.name,
                "Description": line.Description, "Quantity": line.ItemBasedExpenseLineDetail.get("Qty"),
                "Cost": line.ItemBasedExpenseLineDetail.get("UnitPrice"), "Amount": line.Amount,
                "ClassRef_FullName": line.ItemBasedExpenseLineDetail.ClassRef.name if line.ItemBasedExpenseLineDetail.ClassRef else None,
                "CustomerRef_FullName": line.ItemBasedExpenseLineDetail.CustomerRef.name if line.ItemBasedExpenseLineDetail.CustomerRef else None,
            })
    return header, lines

def transform_journal_entry(je, cid):
    header = {
        "TxnID": je.Id, "CompanyID": cid, "TxnDate": je.TxnDate, "RefNumber": je.DocNumber,
        "IsAdjustment": je.Adjustment, "Memo": je.PrivateNote,
        "LastModifiedUTC": je.MetaData.get('LastUpdatedTime'), "DateCreatedUTC": je.MetaData.get('CreateTime')
    }
    lines = []
    for line in je.Line:
        lines.append({
            "TxnLineID": line.Id, "Parent_TxnID": je.Id, "CompanyID": cid,
            "JournalLineType": line.JournalEntryLineDetail.get("PostingType"),
            "Amount": line.Amount,
            "AccountRef_FullName": line.JournalEntryLineDetail.AccountRef.name,
            "EntityRef_FullName": line.JournalEntryLineDetail.Entity.EntityRef.name if line.JournalEntryLineDetail.Entity else None,
            "ClassRef_FullName": line.JournalEntryLineDetail.ClassRef.name if line.JournalEntryLineDetail.ClassRef else None,
            "Memo": line.Description,
        })
    return header, lines

def transform_receive_payment(p, cid):
    return {
        "TxnID": p.Id, "CompanyID": cid, "CustomerRef_FullName": p.CustomerRef.name,
        "TxnDate": p.TxnDate, "RefNumber": p.PaymentRefNum, "TotalAmount": p.TotalAmt,
        "PaymentMethodRef_FullName": p.PaymentMethodRef.name if p.PaymentMethodRef else None,
        "Memo": p.PrivateNote, "UnusedPayment": p.UnappliedAmt,
        "LastModifiedUTC": p.MetaData.get('LastUpdatedTime'), "DateCreatedUTC": p.MetaData.get('CreateTime')
    }

def transform_bill_payment(bp, cid):
    return {
        "TxnID": bp.Id, "CompanyID": cid, "VendorRef_FullName": bp.VendorRef.name,
        "TxnDate": bp.TxnDate, "RefNumber": bp.DocNumber, "Amount": bp.TotalAmt,
        "PayType": bp.PayType,
        "BankAccountRef_FullName": bp.CheckPayment.BankAccountRef.name if bp.PayType == 'Check' and bp.CheckPayment else (bp.CreditCardPayment.CreditCardAccountRef.name if bp.PayType == 'CreditCard' and bp.CreditCardPayment else None),
        "Memo": bp.PrivateNote,
        "LastModifiedUTC": bp.MetaData.get('LastUpdatedTime'), "DateCreatedUTC": bp.MetaData.get('CreateTime')
    }

def transform_sales_order(so, cid):
    header = {
        "TxnID": so.Id, "CompanyID": cid, "CustomerRef_FullName": so.CustomerRef.name,
        "TxnDate": so.TxnDate, "RefNumber": so.DocNumber, "TotalAmount": so.TotalAmt,
        "IsFullyInvoiced": so.POStatus == 'FullyInvoiced', "IsManuallyClosed": so.ManuallyClosed,
        "Memo": so.PrivateNote,
        "LastModifiedUTC": so.MetaData.get('LastUpdatedTime'), "DateCreatedUTC": so.MetaData.get('CreateTime')
    }
    lines = []
    for line in so.Line:
        if line.SalesItemLineDetail:
            lines.append({
                "TxnLineID": line.Id, "Parent_TxnID": so.Id, "CompanyID": cid,
                "ItemRef_FullName": line.SalesItemLineDetail.ItemRef.name,
                "Description": line.Description, "Quantity": line.SalesItemLineDetail.get("Qty"),
                "Rate": line.SalesItemLineDetail.get("UnitPrice"), "Amount": line.Amount
            })
    return header, lines

def transform_purchase_order(po, cid):
    header = {
        "TxnID": po.Id, "CompanyID": cid, "VendorRef_FullName": po.VendorRef.name,
        "TxnDate": po.TxnDate, "RefNumber": po.DocNumber, "TotalAmount": po.TotalAmt,
        "IsFullyReceived": po.POStatus == 'Closed', "IsManuallyClosed": po.ManuallyClosed,
        "Memo": po.PrivateNote,
        "LastModifiedUTC": po.MetaData.get('LastUpdatedTime'), "DateCreatedUTC": po.MetaData.get('CreateTime')
    }
    lines = []
    for line in po.Line:
        if line.ItemBasedExpenseLineDetail:
            lines.append({
                "TxnLineID": line.Id, "Parent__TxnID": po.Id, "CompanyID": cid,
                "ItemRef_FullName": line.ItemBasedExpenseLineDetail.ItemRef.name,
                "Description": line.Description, "Quantity": line.ItemBasedExpenseLineDetail.get("Qty"),
                "Rate": line.ItemBasedExpenseLineDetail.get("UnitPrice"), "Amount": line.Amount
            })
    return header, lines
    
