IF COL_LENGTH('dbo.backlogitem', 'bi_version') IS NULL
BEGIN
    ALTER TABLE dbo.backlogitem
    ADD bi_version NVARCHAR(20) NULL;
END;
GO
