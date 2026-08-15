-- Add license columns to [user] table
-- Run this script against the database to create the new columns

ALTER TABLE dbo.[user]
ADD 
    u_licensenumber varchar(50) NULL,
    u_licensestate varchar(2) NULL,
    u_licenseexpiration date NULL;

-- Verify the columns were created
SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_NAME = 'user' 
AND COLUMN_NAME IN ('u_licensenumber', 'u_licensestate', 'u_licenseexpiration')
ORDER BY ORDINAL_POSITION;
