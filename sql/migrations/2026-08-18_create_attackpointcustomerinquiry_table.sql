-- Attack point brackets for Customer Inquiry counts. Mirrors AttackPointStatus:
-- the scoring query picks the highest apci_count bracket that the service request's
-- inquiry count (during the current secondary status) meets or exceeds.
--
-- No sentinel row: zero inquiries simply matches no bracket and scores 0.
-- Seed values are defaults; admins tune them from the office settings UI.

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'AttackPointCustomerInquiry')
BEGIN
    CREATE TABLE dbo.AttackPointCustomerInquiry (
        apci_id               INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        apci_insertdatetime   DATETIME      NOT NULL CONSTRAINT DF_AttackPointCustomerInquiry_insertdatetime DEFAULT (GETDATE()),
        apci_modifieddatetime DATETIME      NULL,
        apci_description      NVARCHAR(200) NULL,
        apci_count            INT           NOT NULL,   -- minimum inquiry count for this bracket
        apci_attack           INT           NOT NULL    -- attack points awarded at this bracket
    );
END;
GO

IF NOT EXISTS (SELECT 1 FROM dbo.AttackPointCustomerInquiry)
BEGIN
    INSERT INTO dbo.AttackPointCustomerInquiry (apci_description, apci_count, apci_attack)
    VALUES
        ('First customer inquiry during current status', 1, 25),
        ('Second customer inquiry during current status', 2, 50),
        ('Three or more customer inquiries during current status', 3, 100);
END;
GO
