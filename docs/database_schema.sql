-- PACS DataBridge Database Schema
-- Version 1.0

-- Create DataBridge database
-- USE master;
-- GO
-- IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'DataBridge')
-- BEGIN
--     CREATE DATABASE DataBridge;
-- END
-- GO
-- USE DataBridge;
-- GO

-- Import tracking table
CREATE TABLE ImportJob (
    ImportJobID INT IDENTITY(1,1) PRIMARY KEY,
    JobName NVARCHAR(100) NOT NULL,
    ImportType NVARCHAR(50) NOT NULL, -- 'Permit', 'PersonalProperty', etc.
    SourceFile NVARCHAR(500) NOT NULL,
    ImportDate DATETIME2 NOT NULL DEFAULT GETDATE(),
    CompletedDate DATETIME2 NULL,
    Status NVARCHAR(50) NOT NULL DEFAULT 'Pending', -- 'Pending', 'Processing', 'Completed', 'Failed'
    RecordsTotal INT NOT NULL DEFAULT 0,
    RecordsProcessed INT NOT NULL DEFAULT 0,
    RecordsSuccessful INT NOT NULL DEFAULT 0,
    RecordsFailed INT NOT NULL DEFAULT 0,
    UserID NVARCHAR(50) NULL,
    ErrorMessage NVARCHAR(MAX) NULL,
    CreatedDate DATETIME2 NOT NULL DEFAULT GETDATE(),
    ModifiedDate DATETIME2 NOT NULL DEFAULT GETDATE()
);

-- Import error log
CREATE TABLE ImportError (
    ImportErrorID INT IDENTITY(1,1) PRIMARY KEY,
    ImportJobID INT NOT NULL FOREIGN KEY REFERENCES ImportJob(ImportJobID),
    RecordIndex INT NULL, -- Row number in source file
    ErrorType NVARCHAR(50) NOT NULL, -- 'Validation', 'Processing', 'Database', etc.
    ErrorMessage NVARCHAR(MAX) NOT NULL,
    RecordData NVARCHAR(MAX) NULL, -- JSON representation of the record data
    CreatedDate DATETIME2 NOT NULL DEFAULT GETDATE()
);

-- Import field mapping
CREATE TABLE FieldMapping (
    FieldMappingID INT IDENTITY(1,1) PRIMARY KEY,
    MappingName NVARCHAR(100) NOT NULL,
    ImportType NVARCHAR(50) NOT NULL, -- 'Permit', 'PersonalProperty', etc.
    IsDefault BIT NOT NULL DEFAULT 0,
    MappingConfig NVARCHAR(MAX) NOT NULL, -- JSON configuration of field mappings
    CreatedDate DATETIME2 NOT NULL DEFAULT GETDATE(),
    ModifiedDate DATETIME2 NOT NULL DEFAULT GETDATE(),
    UserID NVARCHAR(50) NULL
);

-- Validation rules
CREATE TABLE ValidationRule (
    ValidationRuleID INT IDENTITY(1,1) PRIMARY KEY,
    RuleName NVARCHAR(100) NOT NULL,
    ImportType NVARCHAR(50) NOT NULL, -- 'Permit', 'PersonalProperty', etc.
    FieldName NVARCHAR(100) NOT NULL,
    RuleType NVARCHAR(50) NOT NULL, -- 'Required', 'Format', 'Range', 'Custom', etc.
    RuleConfig NVARCHAR(MAX) NOT NULL, -- JSON configuration of rule parameters
    ErrorMessage NVARCHAR(500) NOT NULL,
    IsActive BIT NOT NULL DEFAULT 1,
    CreatedDate DATETIME2 NOT NULL DEFAULT GETDATE(),
    ModifiedDate DATETIME2 NOT NULL DEFAULT GETDATE()
);

-- Permit data staging table
CREATE TABLE PermitStaging (
    PermitStagingID INT IDENTITY(1,1) PRIMARY KEY,
    ImportJobID INT NOT NULL FOREIGN KEY REFERENCES ImportJob(ImportJobID),
    PermitNumber NVARCHAR(50) NULL,
    PermitType NVARCHAR(50) NULL,
    IssueDate DATE NULL,
    Description NVARCHAR(500) NULL,
    ProjectAddress NVARCHAR(200) NULL,
    ParcelNumber NVARCHAR(50) NULL,
    ApplicantName NVARCHAR(100) NULL,
    ApplicantCompany NVARCHAR(100) NULL,
    ApplicantPhone NVARCHAR(20) NULL,
    ApplicantEmail NVARCHAR(100) NULL,
    WorkDescription NVARCHAR(MAX) NULL,
    ValuationAmount DECIMAL(18, 2) NULL,
    SquareFootage INT NULL,
    Jurisdiction NVARCHAR(100) NULL,
    Status NVARCHAR(50) NULL,
    ImprovementType NVARCHAR(50) NULL, -- 'NEW_CONSTRUCTION', 'ADDITION', 'REMODEL', etc.
    SourceData NVARCHAR(MAX) NULL, -- JSON of original data
    ValidationStatus NVARCHAR(50) NOT NULL DEFAULT 'Pending', -- 'Pending', 'Valid', 'Invalid', 'Warning'
    ValidationMessage NVARCHAR(MAX) NULL,
    ProcessingStatus NVARCHAR(50) NOT NULL DEFAULT 'Pending', -- 'Pending', 'Processed', 'Failed', 'Skipped'
    ProcessingMessage NVARCHAR(MAX) NULL,
    CreatedDate DATETIME2 NOT NULL DEFAULT GETDATE(),
    ModifiedDate DATETIME2 NOT NULL DEFAULT GETDATE()
);

-- Personal property staging table
CREATE TABLE PersonalPropertyStaging (
    PersonalPropertyStagingID INT IDENTITY(1,1) PRIMARY KEY,
    ImportJobID INT NOT NULL FOREIGN KEY REFERENCES ImportJob(ImportJobID),
    TaxpayerID NVARCHAR(50) NULL,
    BusinessName NVARCHAR(200) NULL,
    TaxpayerName NVARCHAR(200) NULL,
    PropertyAddress NVARCHAR(200) NULL,
    MailingAddress NVARCHAR(200) NULL,
    City NVARCHAR(100) NULL,
    State NVARCHAR(50) NULL,
    ZipCode NVARCHAR(20) NULL,
    ParcelNumber NVARCHAR(50) NULL,
    PropertyType NVARCHAR(50) NULL, -- 'COMPUTER_EQUIPMENT', 'FURNITURE', 'MACHINERY_EQUIPMENT', etc.
    Description NVARCHAR(500) NULL,
    AcquisitionDate DATE NULL,
    AcquisitionCost DECIMAL(18, 2) NULL,
    Quantity INT NULL,
    Year INT NULL,
    Make NVARCHAR(100) NULL,
    Model NVARCHAR(100) NULL,
    SerialNumber NVARCHAR(100) NULL,
    Condition NVARCHAR(50) NULL,
    Category NVARCHAR(50) NULL,
    SourceData NVARCHAR(MAX) NULL, -- JSON of original data
    ValidationStatus NVARCHAR(50) NOT NULL DEFAULT 'Pending', -- 'Pending', 'Valid', 'Invalid', 'Warning'
    ValidationMessage NVARCHAR(MAX) NULL,
    ProcessingStatus NVARCHAR(50) NOT NULL DEFAULT 'Pending', -- 'Pending', 'Processed', 'Failed', 'Skipped'
    ProcessingMessage NVARCHAR(MAX) NULL,
    CreatedDate DATETIME2 NOT NULL DEFAULT GETDATE(),
    ModifiedDate DATETIME2 NOT NULL DEFAULT GETDATE()
);

-- Address matching results
CREATE TABLE AddressMatch (
    AddressMatchID INT IDENTITY(1,1) PRIMARY KEY,
    SourceAddress NVARCHAR(200) NOT NULL,
    StandardizedAddress NVARCHAR(200) NULL,
    ParcelNumber NVARCHAR(50) NULL,
    ConfidenceScore DECIMAL(5, 2) NULL,
    MatchMethod NVARCHAR(50) NULL, -- 'Exact', 'Fuzzy', 'Manual', etc.
    MatchDate DATETIME2 NOT NULL DEFAULT GETDATE(),
    UserID NVARCHAR(50) NULL, -- If manually matched
    ImportJobID INT NULL FOREIGN KEY REFERENCES ImportJob(ImportJobID),
    RecordType NVARCHAR(50) NULL, -- 'Permit', 'PersonalProperty', etc.
    RecordID INT NULL -- Reference to PermitStagingID or PersonalPropertyStagingID
);

-- Configuration table
CREATE TABLE ConfigSetting (
    ConfigSettingID INT IDENTITY(1,1) PRIMARY KEY,
    SettingCategory NVARCHAR(50) NOT NULL,
    SettingName NVARCHAR(100) NOT NULL,
    SettingValue NVARCHAR(MAX) NULL,
    IsEncrypted BIT NOT NULL DEFAULT 0,
    Description NVARCHAR(500) NULL,
    CreatedDate DATETIME2 NOT NULL DEFAULT GETDATE(),
    ModifiedDate DATETIME2 NOT NULL DEFAULT GETDATE()
);

-- User management (if not using external authentication)
CREATE TABLE AppUser (
    UserID NVARCHAR(50) PRIMARY KEY,
    Username NVARCHAR(100) NOT NULL UNIQUE,
    PasswordHash NVARCHAR(200) NULL,
    Email NVARCHAR(100) NULL,
    FullName NVARCHAR(100) NULL,
    Department NVARCHAR(100) NULL,
    Role NVARCHAR(50) NOT NULL DEFAULT 'User', -- 'Admin', 'User', 'Viewer', etc.
    IsActive BIT NOT NULL DEFAULT 1,
    LastLoginDate DATETIME2 NULL,
    CreatedDate DATETIME2 NOT NULL DEFAULT GETDATE(),
    ModifiedDate DATETIME2 NOT NULL DEFAULT GETDATE()
);

-- Audit log
CREATE TABLE AuditLog (
    AuditLogID INT IDENTITY(1,1) PRIMARY KEY,
    EventType NVARCHAR(50) NOT NULL, -- 'Login', 'Import', 'Export', 'Config', etc.
    EventDate DATETIME2 NOT NULL DEFAULT GETDATE(),
    UserID NVARCHAR(50) NULL,
    IPAddress NVARCHAR(50) NULL,
    Description NVARCHAR(500) NOT NULL,
    DetailsJSON NVARCHAR(MAX) NULL -- Additional event details in JSON
);

-- Data transformation rules
CREATE TABLE TransformationRule (
    TransformationRuleID INT IDENTITY(1,1) PRIMARY KEY,
    RuleName NVARCHAR(100) NOT NULL,
    ImportType NVARCHAR(50) NOT NULL, -- 'Permit', 'PersonalProperty', etc.
    FieldName NVARCHAR(100) NOT NULL,
    RuleType NVARCHAR(50) NOT NULL, -- 'Mapping', 'Format', 'Calculate', 'Custom', etc.
    TransformationLogic NVARCHAR(MAX) NOT NULL, -- SQL, Python code, or rule configuration
    ExecutionOrder INT NOT NULL DEFAULT 100,
    IsActive BIT NOT NULL DEFAULT 1,
    CreatedDate DATETIME2 NOT NULL DEFAULT GETDATE(),
    ModifiedDate DATETIME2 NOT NULL DEFAULT GETDATE()
);

-- Indexes for performance
CREATE INDEX IX_PermitStaging_ImportJobID ON PermitStaging(ImportJobID);
CREATE INDEX IX_PermitStaging_ParcelNumber ON PermitStaging(ParcelNumber);
CREATE INDEX IX_PermitStaging_ProcessingStatus ON PermitStaging(ProcessingStatus);
CREATE INDEX IX_PersonalPropertyStaging_ImportJobID ON PersonalPropertyStaging(ImportJobID);
CREATE INDEX IX_PersonalPropertyStaging_ParcelNumber ON PersonalPropertyStaging(ParcelNumber);
CREATE INDEX IX_PersonalPropertyStaging_ProcessingStatus ON PersonalPropertyStaging(ProcessingStatus);
CREATE INDEX IX_ImportJob_Status ON ImportJob(Status);
CREATE INDEX IX_ImportJob_ImportType ON ImportJob(ImportType);
CREATE INDEX IX_ImportError_ImportJobID ON ImportError(ImportJobID);
CREATE INDEX IX_AddressMatch_ParcelNumber ON AddressMatch(ParcelNumber);
CREATE INDEX IX_AddressMatch_SourceAddress ON AddressMatch(SourceAddress);

-- Sample data for testing
-- INSERT INTO ConfigSetting (SettingCategory, SettingName, SettingValue, Description)
-- VALUES 
-- ('Database', 'PACSServer', 'localhost', 'PACS SQL Server instance name'),
-- ('Database', 'PACSDatabase', 'PACS', 'PACS database name'),
-- ('Import', 'PermitWatchFolder', 'C:\PACS\Permits\Import', 'Folder to monitor for new permit files'),
-- ('Import', 'PermitArchiveFolder', 'C:\PACS\Permits\Archive', 'Folder to archive processed permit files'),
-- ('Import', 'PersonalPropertyWatchFolder', 'C:\PACS\PersonalProperty\Import', 'Folder to monitor for new personal property files'),
-- ('Import', 'PersonalPropertyArchiveFolder', 'C:\PACS\PersonalProperty\Archive', 'Folder to archive processed personal property files');

-- Sample Views
-- View for permit import statistics
CREATE VIEW vw_PermitImportStats AS
SELECT 
    ImportType,
    COUNT(*) AS TotalJobs,
    SUM(RecordsTotal) AS TotalRecords,
    SUM(RecordsSuccessful) AS SuccessfulRecords,
    SUM(RecordsFailed) AS FailedRecords,
    CAST(SUM(RecordsSuccessful) * 100.0 / NULLIF(SUM(RecordsTotal), 0) AS DECIMAL(5,2)) AS SuccessRate,
    AVG(DATEDIFF(SECOND, ImportDate, CompletedDate)) AS AvgProcessingTimeSeconds
FROM 
    ImportJob
WHERE 
    ImportType = 'Permit'
    AND CompletedDate IS NOT NULL
GROUP BY 
    ImportType;

-- View for address matching statistics
CREATE VIEW vw_AddressMatchStats AS
SELECT 
    MatchMethod,
    COUNT(*) AS TotalMatches,
    AVG(ConfidenceScore) AS AvgConfidence,
    COUNT(CASE WHEN ParcelNumber IS NOT NULL THEN 1 END) AS SuccessfulMatches,
    COUNT(CASE WHEN ParcelNumber IS NULL THEN 1 END) AS FailedMatches,
    CAST(COUNT(CASE WHEN ParcelNumber IS NOT NULL THEN 1 END) * 100.0 / COUNT(*) AS DECIMAL(5,2)) AS SuccessRate
FROM 
    AddressMatch
GROUP BY 
    MatchMethod;

-- Stored procedures
-- Check for duplicate permit records
CREATE PROCEDURE usp_CheckDuplicatePermits
    @ImportJobID INT
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Identify potential duplicates based on permit number
    SELECT 
        p1.PermitStagingID,
        p1.PermitNumber,
        p1.ProjectAddress,
        p1.ParcelNumber,
        p1.IssueDate,
        'Duplicate permit number found in system' AS ValidationMessage
    FROM 
        PermitStaging p1
    JOIN 
        PermitStaging p2 ON p1.PermitNumber = p2.PermitNumber
                        AND p1.PermitStagingID <> p2.PermitStagingID
    WHERE 
        p1.ImportJobID = @ImportJobID
        AND p2.ProcessingStatus <> 'Failed';
    
    -- Update validation status for duplicates
    UPDATE p
    SET 
        ValidationStatus = 'Warning',
        ValidationMessage = ISNULL(ValidationMessage, '') + 'Duplicate permit number found in system; '
    FROM 
        PermitStaging p
    WHERE 
        p.ImportJobID = @ImportJobID
        AND EXISTS (
            SELECT 1 
            FROM PermitStaging p2 
            WHERE p.PermitNumber = p2.PermitNumber 
            AND p.PermitStagingID <> p2.PermitStagingID
            AND p2.ProcessingStatus <> 'Failed'
        );
END;

-- Mark import job as completed
CREATE PROCEDURE usp_CompleteImportJob
    @ImportJobID INT
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @RecordsTotal INT,
            @RecordsProcessed INT,
            @RecordsSuccessful INT,
            @RecordsFailed INT;
    
    -- Get counts for permit imports
    IF EXISTS (SELECT 1 FROM ImportJob WHERE ImportJobID = @ImportJobID AND ImportType = 'Permit')
    BEGIN
        SELECT 
            @RecordsTotal = COUNT(*),
            @RecordsProcessed = COUNT(CASE WHEN ProcessingStatus IN ('Processed', 'Failed', 'Skipped') THEN 1 END),
            @RecordsSuccessful = COUNT(CASE WHEN ProcessingStatus = 'Processed' THEN 1 END),
            @RecordsFailed = COUNT(CASE WHEN ProcessingStatus IN ('Failed', 'Skipped') THEN 1 END)
        FROM 
            PermitStaging
        WHERE 
            ImportJobID = @ImportJobID;
    END
    
    -- Get counts for personal property imports
    ELSE IF EXISTS (SELECT 1 FROM ImportJob WHERE ImportJobID = @ImportJobID AND ImportType = 'PersonalProperty')
    BEGIN
        SELECT 
            @RecordsTotal = COUNT(*),
            @RecordsProcessed = COUNT(CASE WHEN ProcessingStatus IN ('Processed', 'Failed', 'Skipped') THEN 1 END),
            @RecordsSuccessful = COUNT(CASE WHEN ProcessingStatus = 'Processed' THEN 1 END),
            @RecordsFailed = COUNT(CASE WHEN ProcessingStatus IN ('Failed', 'Skipped') THEN 1 END)
        FROM 
            PersonalPropertyStaging
        WHERE 
            ImportJobID = @ImportJobID;
    END
    
    -- Update import job with final stats
    UPDATE ImportJob
    SET 
        Status = 'Completed',
        CompletedDate = GETDATE(),
        RecordsTotal = @RecordsTotal,
        RecordsProcessed = @RecordsProcessed,
        RecordsSuccessful = @RecordsSuccessful,
        RecordsFailed = @RecordsFailed,
        ModifiedDate = GETDATE()
    WHERE 
        ImportJobID = @ImportJobID;
END;
