/*SCHEMAS*/

--Create schemas for organization and security
--Quickbooks Schema
BEGIN TRAN;
IF NOT EXISTS(SELECT * FROM sys.schemas WHERE name = 'qb_data')
BEGIN
	EXEC('CREATE SCHEMA qb_data');
	PRINT 'Schema "qb_data" created.';
END
GO
--Database Control Schema
IF NOT EXISTS(SELECT * FROM sys.schemas WHERE name = 'etl')
BEGIN 
	EXEC('CREATE SCHEMA ETL');
	PRINT'Schema "etl" created.';
END
GO
COMMIT;

--Create Database Control Table to manage which companies are processed
CREATE TABLE etl.Companies(
	CompanyID NVARCHAR(50) PRIMARY KEY NOT NULL,
	CompanyName NVARCHAR(255) NOT NULL,
	QBFilePath NVARCHAR(MAX) NOT NULL,
	IsActive BIT NOT NULL DEFAULT 1,
	LastSyncStatus NVARCHAR(50),
	LastSyncTimeUTC DATETIME2,
	LastSyncMessage NVARCHAR(MAX)
);
GO
PRINT 'Table "etl.Companies" created.';

--Populate the table with company file details
/*INSERT INTO etl.Companies(
	CompanyID,
	CompanyName,
	QBFilePath,
	IsActive 
VALUES
(),
();
GO
PRINT 'company details inserted into etl.Companies';*/

/*LIST TABLES (Who, What, How)*/
--Create Accounts Table
CREATE TABLE qb_data.Accounts(
	ListID NVARCHAR(100) NOT NULL, 
	CompanyID NVARCHAR(50) NOT NULL, 
	Name NVARCHAR(255), 
	FullName NVARCHAR(1500), 
	IsActive BIT, 
	ParentRef_ListID NVARCHAR(100), 
	Sublevel INT, 
	AccountType NVARCHAR(100), 
	DetailType NVARCHAR(100), 
	AccountNumber NVARCHAR(100), 
	BankNumber NVARCHAR(100), 
	RoutingNumber NVARCHAR(100), 
	Description NVARCHAR(MAX), 
	Balance MONEY, 
	TotalBalance MONEY, 
	LastModifiedUTC DATETIME2, 
	DateCreatedUTC DATETIME2, 
	PRIMARY KEY(ListID, CompanyID));
PRINT 'Table "qb_data.Accounts" created.';


--Create Customers Table
CREATE TABLE qb_data.Customers(
	ListID NVARCHAR(100) NOT NULL, 
	CompanyID NVARCHAR(50) NOT NULL, 
	FullName NVARCHAR(255), 
	CompanyName NVARCHAR(255), 
	FirstName NVARCHAR(100), 
	MiddleName NVARCHAR(100), 
	LastName NVARCHAR(100), 
	IsActive BIT, 
	ParentRef_ListID NVARCHAR(100), 
	Sublevel INT, 
	BillAddress_Line1 NVARCHAR(255), 
	BillAddress_City NVARCHAR(100), 
	BillAddress_State NVARCHAR(50), 
	BillAddress_PostalCode NVARCHAR(50), 
	ShipAddress_Line1 NVARCHAR(255), 
	ShipAddress_City NVARCHAR(100), 
	ShipAddress_State NVARCHAR(50), 
	ShipAddress_PostalCode NVARCHAR(50), 
	Email NVARCHAR(255), 
	Phone NVARCHAR(50),
	AltPhone NVARCHAR(50), 
	Fax NVARCHAR(50), 
	Balance MONEY, 
	TotalBalance MONEY, 
	CreditLimit MONEY, 
	JobStatus NVARCHAR(50), 
	LastModifiedUTC DATETIME2,
	DateCreatedUTC DATETIME2, 
	PRIMARY KEY(ListID, CompanyID));
PRINT 'Table "qb_data.Customers" created.';

--Create Vendors Table
CREATE TABLE qb_data.Vendors(
	ListID NVARCHAR(100) NOT NULL, 
	CompanyID NVARCHAR(50) NOT NULL, 
	Name NVARCHAR(255), 
	CompanyName NVARCHAR(255),
	FirstName NVARCHAR(100), 
	LastName NVARCHAR(100), 
	IsActive BIT, 
	Email NVARCHAR(255), 
	Phone NVARCHAR(50), 
	Balance MONEY, 
	VendorTaxIdent NVARCHAR(50), 
	Is1099Eligible BIT, 
	LastModifiedUTC DATETIME2,
	DateCreatedUTC DATETIME2, 
	PRIMARY KEY (ListID, CompanyID) );
PRINT 'Table "qb_data.Vendors" created.';

--Create Employees Table
CREATE TABLE qb_data.Employees(
	ListID NVARCHAR(100) NOT NULL,
	CompanyID NVARCHAR(50) NOT NULL,
	FirstName NVARCHAR(100), 
	MiddleName NVARCHAR(100),
	LastName NVARCHAR(100),
	FullName NVARCHAR(255),
	IsActive BIT, 
	EmployeeType NVARCHAR(50), 
	Gender NVARCHAR(20), 
	HiredDate DATE, 
	ReleasedDate DATE,
	BirthDate DATE, 
	LastModifiedUTC DATETIME2, 
	DateCreatedUTC DATETIME2, 
	PRIMARY KEY(ListID, CompanyID));
PRINT 'Table "qb_data.Employees" created.';

--Create Items(Product) Table
CREATE TABLE qb_data.Items(
	ListID NVARCHAR(100) NOT NULL,
	CompanyID NVARCHAR(50) NOT NULL, 
	FullName NVARCHAR(1500), 
	IsActive BIT,
	ItemType NVARCHAR(100), 
	SalesDesc NVARCHAR(MAX), 
	SalesPrice MONEY, 
	PurchaseDesc NVARCHAR(MAX),
	PurchaseCost MONEY, 
	QuantityOnHand FLOAT, 
	QuantityOnOrder FLOAT, 
	QuantityOnSalesOrder FLOAT, 
	AverageCost MONEY, 
	ParentRef_ListID NVARCHAR(100), 
	IncomeAccountRef_FullName NVARCHAR(255), 
	ExpenseAccountRef_FullName NVARCHAR(255), 
	AssetAccountRef_FullName NVARCHAR(255),
	LastModifiedUTC DATETIME2, 
	DateCreatedUTC DATETIME2, 
	PRIMARY KEY(ListID, CompanyID));
PRINT 'Table "qb_data.Items" created.';

--Create Classes Table
CREATE TABLE qb_data.Classes(
	ListID NVARCHAR(100) NOT NULL, 
	CompanyID NVARCHAR(50) NOT NULL, 
	FullName NVARCHAR(255), 
	IsActive BIT, 
	ParentRef_ListID NVARCHAR(100), 
	LastModifiedUTC DATETIME2, 
	DateCreatedUTC DATETIME2, 
	PRIMARY KEY(ListID, CompanyID));
PRINT 'Table "qb_data.Classes" created.';

/*TRANSACTION TABLES(When, Why)*/
--Invoices 
CREATE TABLE qb_data.Invoices(
	TxnID NVARCHAR(100) NOT NULL, 
	CompanyID NVARCHAR(50) NOT NULL,
	CustomerRef_ListID NVARCHAR(100), 
	CustomerRef_FullName NVARCHAR(255), 
	ClassRef_FullName NVARCHAR(255), 
	TxnDate DATE, 
	RefNumber NVARCHAR(50), 
	DueDate DATE, ShipDate DATE, 
	Subtotal MONEY,
	SalesTaxTotal MONEY,
	AppliedAmount MONEY, 
	BalanceRemaining MONEY,
	TotalAmount MONEY, 
	IsPaid BIT, 
	IsPending BIT, 
	Memo NVARCHAR(MAX),
	LastModifiedUTC DATETIME2, 
	DateCreatedUTC DATETIME2, 
	PRIMARY KEY(TxnID, CompanyID));
CREATE TABLE qb_data.InvoiceLines(
	TxnLineID NVARCHAR(100) NOT NULL,
	Parent_TxnID NVARCHAR(100) NOT NULL,
	CompanyID NVARCHAR(50) NOT NULL, 
	ItemRef_ListID NVARCHAR(100), 
	ItemRef_FullName NVARCHAR(1500), 
	Description NVARCHAR(MAX), 
	Quantity FLOAT,
	Rate MONEY,
	Amount MONEY, 
	ClassRef_FullName NVARCHAR(255), 
	SalesTaxCodeRef_FullName NVARCHAR(50), 
	PRIMARY KEY(TxnLineID, CompanyID));
PRINT 'Tables "qb_data.Invoices" and "qb_data.InvoiceLines" created.';

-- Bills
CREATE TABLE qb_data.Bills(
	TxnID NVARCHAR(100) NOT NULL, 
	CompanyID NVARCHAR(50) NOT NULL, 
	VendorRef_ListID NVARCHAR(100), 
	VendorRef_FullName NVARCHAR(255), 
	TxnDate DATE, DueDate DATE, 
	RefNumber NVARCHAR(50), 
	AmountDue MONEY, IsPaid BIT, 
	Memo NVARCHAR(MAX), 
	LastModifiedUTC DATETIME2, 
	DateCreatedUTC DATETIME2, 
	PRIMARY KEY(TxnID, CompanyID));
CREATE TABLE qb_data.BillLines(
	TxnLineID NVARCHAR(100) NOT NULL,
	Parent_TxnID NVARCHAR(100) NOT NULL, 
	CompanyID NVARCHAR(50) NOT NULL, 
	ItemRef_ListID NVARCHAR(100), 
	ItemRef_FullName NVARCHAR(1500),
	Description NVARCHAR(MAX), 
	Quantity FLOAT, 
	Cost MONEY, Amount MONEY, 
	ClassRef_FullName NVARCHAR(255), 
	CustomerRef_FullName NVARCHAR(255), 
	PRIMARY KEY(TxnLineID, CompanyID));
PRINT 'Tables "qb_data.Bills" and "qb_data.BillLines" created.';

-- Journal Entries
CREATE TABLE qb_data.JournalEntries(
	TxnID NVARCHAR(100) NOT NULL, 
	CompanyID NVARCHAR(50) NOT NULL, 
	TxnDate DATE, 
	RefNumber NVARCHAR(50),
	IsAdjustment BIT, 
	Memo NVARCHAR(MAX), 
	LastModifiedUTC DATETIME2, 
	DateCreatedUTC DATETIME2, 
	PRIMARY KEY(TxnID, CompanyID));
CREATE TABLE qb_data.JournalEntryLines(
	TxnLineID NVARCHAR(100) NOT NULL, 
	Parent_TxnID NVARCHAR(100) NOT NULL, 
	CompanyID NVARCHAR(50) NOT NULL, 
	JournalLineType NVARCHAR(10), 
	Amount MONEY,
	AccountRef_FullName NVARCHAR(255), 
	EntityRef_FullName NVARCHAR(255), 
	ClassRef_FullName NVARCHAR(255), 
	Memo NVARCHAR(MAX),
	PRIMARY KEY (TxnLineID, CompanyID) );
PRINT 'Tables "qb_data.JournalEntries" and "qb_data.JournalEntryLines" created.';

-- Payments Received
CREATE TABLE qb_data.ReceivePayments(
	TxnID NVARCHAR(100) NOT NULL, 
	CompanyID NVARCHAR(50) NOT NULL, 
	CustomerRef_FullName NVARCHAR(255), 
	TxnDate DATE, 
	RefNumber NVARCHAR(50), 
	TotalAmount MONEY, 
	PaymentMethodRef_FullName NVARCHAR(255), 
	Memo NVARCHAR(MAX), 
	UnusedPayment MONEY, 
	LastModifiedUTC DATETIME2, 
	DateCreatedUTC DATETIME2, 
	PRIMARY KEY(TxnID, CompanyID));
PRINT 'Table "qb_data.ReceivePayments" created.';

-- Bills Paid
CREATE TABLE qb_data.BillPayments(
	TxnID NVARCHAR(100) NOT NULL, 
	CompanyID NVARCHAR(50) NOT NULL,
	VendorRef_FullName NVARCHAR(255),
	TxnDate DATE, 
	RefNumber NVARCHAR(50), 
	Amount MONEY, 
	PayType NVARCHAR(50), 
	BankAccountRef_FullName NVARCHAR(255),
	Memo NVARCHAR(MAX),
	LastModifiedUTC DATETIME2,
	DateCreatedUTC DATETIME2, 
	PRIMARY KEY(TxnID, CompanyID));
PRINT 'Table "qb_data.BillPayments" created.';

-- Sales Orders
CREATE TABLE qb_data.SalesOrders(
	TxnID NVARCHAR(100) NOT NULL, 
	CompanyID NVARCHAR(50) NOT NULL,
	CustomerRef_FullName NVARCHAR(255),
	TxnDate DATE,
	RefNumber NVARCHAR(50),
	TotalAmount MONEY,
	IsFullyInvoiced BIT,
	IsManuallyClosed BIT,
	Memo NVARCHAR(MAX), 
	LastModifiedUTC DATETIME2,
	DateCreatedUTC DATETIME2, 
	PRIMARY KEY(TxnID, CompanyID));
CREATE TABLE qb_data.SalesOrderLines(
	TxnLineID NVARCHAR(100) NOT NULL,
	Parent_TxnID NVARCHAR(100) NOT NULL, 
	CompanyID NVARCHAR(50) NOT NULL,
	ItemRef_FullName NVARCHAR(1500),
	Description NVARCHAR(MAX), 
	Quantity FLOAT,
	Rate MONEY, 
	Amount MONEY,
	PRIMARY KEY(TxnLineID, CompanyID));
PRINT 'Tables "qb_data.SalesOrders" and "qb_data.SalesOrderLines" created.';

-- Purchase Orders
CREATE TABLE qb_data.PurchaseOrders(
	TxnID NVARCHAR(100) NOT NULL, 
	CompanyID NVARCHAR(50) NOT NULL, 
	VendorRef_FullName NVARCHAR(255), 
	TxnDate DATE,
	RefNumber NVARCHAR(50),
	TotalAmount MONEY,
	IsFullyReceived BIT,
	IsManuallyClosed BIT,
	Memo NVARCHAR(MAX), 
	LastModifiedUTC DATETIME2,
	DateCreatedUTC DATETIME2,
	PRIMARY KEY(TxnID, CompanyID));
CREATE TABLE qb_data.PurchaseOrderLines(
	TxnLineID NVARCHAR(100) NOT NULL, 
	Parent_TxnID NVARCHAR(100) NOT NULL,
	CompanyID NVARCHAR(50) NOT NULL,
	ItemRef_FullName NVARCHAR(1500),
	Description NVARCHAR(MAX),
	Quantity FLOAT,
	Rate MONEY,
	Amount MONEY, 
	PRIMARY KEY (TxnLineID, CompanyID) );
PRINT 'Tables "qb_data.PurchaseOrders" and "qb_data.PurchaseOrderLines" created.';

