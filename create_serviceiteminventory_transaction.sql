-- Inventory modernization: movement history + integrity for dbo.ServiceItemInventory.
--
-- Run this BEFORE deploying the EvoAPI InventoryController / evotech /office/evoinventory page.
--
-- Three parts:
--   1. Make sure sii_modifieddatetime exists (the legacy EvoWS update never wrote it).
--   2. Movement history table so count changes are attributable (who / when / why).
--   3. Duplicate-location guard, applied only if the data is already clean.
--
-- Safe to re-run.

SET NOCOUNT ON;
GO

/* ------------------------------------------------------------------ */
/* 1. sii_modifieddatetime                                            */
/* ------------------------------------------------------------------ */

IF NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID('dbo.ServiceItemInventory') AND name = 'sii_modifieddatetime'
)
BEGIN
    ALTER TABLE dbo.ServiceItemInventory ADD sii_modifieddatetime DATETIME NULL;
    PRINT 'Added dbo.ServiceItemInventory.sii_modifieddatetime';
END
ELSE
    PRINT 'dbo.ServiceItemInventory.sii_modifieddatetime already exists - skipped';
GO

/* ------------------------------------------------------------------ */
/* 2. Movement history                                                */
/* ------------------------------------------------------------------ */
--
-- si_id / sif_id / sir_id are denormalized on purpose: sii_id is set to NULL
-- when an inventory row is deleted, and the history still needs to say what
-- item was at what location. Never join to ServiceItemInventory for those.

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'ServiceItemInventoryTransaction')
BEGIN
    CREATE TABLE dbo.ServiceItemInventoryTransaction (
        siit_id              INT IDENTITY(1,1) PRIMARY KEY,
        sii_id               INT           NULL,        -- NULL once the inventory row is deleted
        si_id                INT           NOT NULL,
        sif_id               INT           NULL,
        sir_id               INT           NULL,
        siit_type            VARCHAR(20)   NOT NULL,    -- Create | Update | Decrement | Delete
        siit_availabledelta  INT           NOT NULL DEFAULT 0,
        siit_allocateddelta  INT           NOT NULL DEFAULT 0,
        siit_availableafter  INT           NULL,        -- resulting counts, NULL for Delete
        siit_allocatedafter  INT           NULL,
        siit_reason          NVARCHAR(500) NULL,
        u_id                 INT           NULL,        -- who; NULL for background/system writes
        siit_username        NVARCHAR(100) NULL,
        siit_insertdatetime  DATETIME      NOT NULL DEFAULT GETDATE()
    );

    CREATE INDEX IX_SIIT_Inventory ON dbo.ServiceItemInventoryTransaction(sii_id, siit_insertdatetime DESC);
    CREATE INDEX IX_SIIT_ServiceItem ON dbo.ServiceItemInventoryTransaction(si_id, siit_insertdatetime DESC);

    PRINT 'Created dbo.ServiceItemInventoryTransaction';
END
ELSE
    PRINT 'dbo.ServiceItemInventoryTransaction already exists - skipped';
GO

-- FK is added separately: it needs a primary key on ServiceItemInventory.sii_id,
-- which the EF model declares but an older database may not actually have. The
-- table works without it - the API nulls sii_id itself on delete - so a missing
-- PK degrades to a warning rather than failing the whole script.
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_ServiceItemInventoryTransaction_Inventory')
BEGIN
    IF EXISTS (
        SELECT 1
        FROM sys.indexes i
        INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
        INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        WHERE i.object_id = OBJECT_ID('dbo.ServiceItemInventory')
          AND (i.is_primary_key = 1 OR i.is_unique = 1)
          AND c.name = 'sii_id'
          AND i.index_id IN (
              SELECT index_id FROM sys.index_columns
              WHERE object_id = OBJECT_ID('dbo.ServiceItemInventory')
              GROUP BY index_id HAVING COUNT(*) = 1
          )
    )
    BEGIN
        ALTER TABLE dbo.ServiceItemInventoryTransaction
            ADD CONSTRAINT FK_ServiceItemInventoryTransaction_Inventory
            FOREIGN KEY (sii_id) REFERENCES dbo.ServiceItemInventory(sii_id) ON DELETE SET NULL;
        PRINT 'Added FK_ServiceItemInventoryTransaction_Inventory';
    END
    ELSE
        PRINT 'No single-column unique/primary key on ServiceItemInventory.sii_id - FK skipped (history still works)';
END
ELSE
    PRINT 'FK_ServiceItemInventoryTransaction_Inventory already exists - skipped';
GO

/* ------------------------------------------------------------------ */
/* 3. Duplicate-location guard                                        */
/* ------------------------------------------------------------------ */
--
-- Two rows for the same item in the same slot make DecrementInventory ambiguous
-- (the legacy version updated every matching row). The API rejects duplicates on
-- create/update; this index makes it structural. SQL Server treats NULLs as equal
-- for uniqueness, which is what we want here - two rows with no shelf/bin at the
-- same rack ARE duplicates.
--
-- Only created if the table is already clean. If it prints a warning, merge the
-- listed rows by hand and re-run.

IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UQ_ServiceItemInventory_Location' AND object_id = OBJECT_ID('dbo.ServiceItemInventory'))
BEGIN
    PRINT 'UQ_ServiceItemInventory_Location already exists - skipped';
END
ELSE IF EXISTS (
    SELECT 1
    FROM dbo.ServiceItemInventory
    GROUP BY si_id, sif_id, sir_id, sii_shelf, sii_bin
    HAVING COUNT(*) > 1
)
BEGIN
    PRINT '*** WARNING: duplicate inventory locations exist - unique index NOT created. ***';
    PRINT '*** Merge the rows listed below, then re-run this script.                   ***';

    SELECT
        sii.si_id,
        si.si_name,
        sii.sif_id,
        sii.sir_id,
        sii.sii_shelf,
        sii.sii_bin,
        COUNT(*)                        AS DuplicateRows,
        SUM(sii.sii_countavailable)     AS TotalAvailable,
        SUM(sii.sii_countallocated)     AS TotalAllocated,
        STRING_AGG(CAST(sii.sii_id AS VARCHAR(20)), ', ') AS InventoryIds
    FROM dbo.ServiceItemInventory sii
    INNER JOIN dbo.ServiceItem si ON sii.si_id = si.si_id
    GROUP BY sii.si_id, si.si_name, sii.sif_id, sii.sir_id, sii.sii_shelf, sii.sii_bin
    HAVING COUNT(*) > 1
    ORDER BY si.si_name;
END
ELSE
BEGIN
    CREATE UNIQUE INDEX UQ_ServiceItemInventory_Location
        ON dbo.ServiceItemInventory(si_id, sif_id, sir_id, sii_shelf, sii_bin);
    PRINT 'Created UQ_ServiceItemInventory_Location';
END
GO

-- Supports the per-item decrement lookup and the "where is this item stocked" query.
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_ServiceItemInventory_ServiceItem' AND object_id = OBJECT_ID('dbo.ServiceItemInventory'))
BEGIN
    CREATE INDEX IX_ServiceItemInventory_ServiceItem ON dbo.ServiceItemInventory(si_id) INCLUDE (sif_id, sir_id, sii_countavailable, sii_countallocated);
    PRINT 'Created IX_ServiceItemInventory_ServiceItem';
END
ELSE
    PRINT 'IX_ServiceItemInventory_ServiceItem already exists - skipped';
GO
