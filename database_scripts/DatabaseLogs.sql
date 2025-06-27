/*SQL SERVER LOG AUDITS*/
--Create SQL server audit for login access
CREATE SERVER AUDIT [Audit_Login_Access]
TO FILE
(   FILEPATH = N'C:\SQL Audits\',  
    MAXSIZE = 100 MB,
    MAX_ROLLOVER_FILES = 5,
    RESERVE_DISK_SPACE = OFF
)
WITH
(   QUEUE_DELAY = 1000,
    ON_FAILURE = CONTINUE
);
GO

--Create Audit Specification to define login events
CREATE SERVER AUDIT SPECIFICATION [Spec_Login_Events]
FOR SERVER AUDIT [Audit_Login_Access]
    ADD (FAILED_LOGIN_GROUP),
	ADD (SUCCESSFUL_LOGIN_GROUP)
WITH (STATE = OFF);
GO

--Enable the audit and specification
ALTER SERVER AUDIT [Audit_Login_Access]
WITH (STATE = ON);
GO

ALTER SERVER AUDIT SPECIFICATION [Spec_Login_Events]
WITH (STATE = ON);
GO

PRINT 'Audit is now fully configured and enabled.';