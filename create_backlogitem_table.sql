IF OBJECT_ID('dbo.backlogitem', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.backlogitem
    (
        bi_id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        bi_code NVARCHAR(50) NOT NULL,
        bi_title NVARCHAR(255) NOT NULL,
        bi_source NVARCHAR(255) NULL,
        bi_author NVARCHAR(150) NULL,
        bi_date DATE NULL,
        bi_category NVARCHAR(100) NULL,
        bi_type NVARCHAR(50) NOT NULL CONSTRAINT DF_backlogitem_type DEFAULT ('Feature'),
        bi_priority NVARCHAR(50) NOT NULL CONSTRAINT DF_backlogitem_priority DEFAULT ('Medium'),
        bi_status NVARCHAR(50) NOT NULL CONSTRAINT DF_backlogitem_status DEFAULT ('New'),
        bi_effort NVARCHAR(20) NULL,
        bi_desc NVARCHAR(MAX) NULL,
        bi_notes NVARCHAR(MAX) NULL,
        bi_detail_markdown NVARCHAR(MAX) NULL,
        bi_history NVARCHAR(MAX) NULL,
        bi_active BIT NOT NULL CONSTRAINT DF_backlogitem_active DEFAULT ((1)),
        bi_u_id_createdby INT NULL,
        bi_u_id_lastupdatedby INT NULL,
        bi_insertdatetime DATETIME NOT NULL CONSTRAINT DF_backlogitem_insertdatetime DEFAULT (GETUTCDATE()),
        bi_lastupdated DATETIME NULL
    );

    CREATE INDEX IX_backlogitem_active_priority_status ON dbo.backlogitem (bi_active, bi_priority, bi_status);
    CREATE INDEX IX_backlogitem_code_title ON dbo.backlogitem (bi_code, bi_title);
    CREATE INDEX IX_backlogitem_date ON dbo.backlogitem (bi_date DESC);
END;

IF COL_LENGTH('dbo.backlogitem', 'bi_notes') IS NULL
BEGIN
    ALTER TABLE dbo.backlogitem
    ADD bi_notes NVARCHAR(MAX) NULL;
END;
