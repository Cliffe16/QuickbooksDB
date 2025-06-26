#Import modules
import win32com.client
from contextlib import contextmanager
import logging

@contextmanager #This decorator turns our function into a context manager
def QBSession(company_file_path):
    """A robust context manager to handle the Quickbooks SDK session lifecycle."""
    session_manager = None
    ticket = None
    try: 
        session_manager = win32com.client.Dispatch("QBXMLRP2.RequestProcessor") #Find the Quickbooks SDK Engine   
        session_manager.OpenConnection("", "Flamingo ETL Service") #Open communication to the engine and name the application
        ticket = session_manager.BeginSession(company_file_path, 2) #Connect to a company's data file irregardless of qbXMLFileMode(Single/Multi-user)
        logging.info(f"Quickbooks SDK session started for file: {company_file_path}") #log the process
        yield session_manager, ticket
    except Exception as e:
        logging.error(f"Failed to start Quickbooks SDK session for {company_file_path}. Error {e}")
        raise
    finally: 
        if ticket and session_manager:
            session_manager.EndSession(ticket)
            logging.info(f"Quickbooks SDK session ended for file: {company_file_path}") #Check if a session was succesfully started and end it
        if session_manager:
            session_manager.CloseConnection()  
            logging.info("Quickbooks SDK connection closed.") #Check if communication to the engine was opened and close it
            