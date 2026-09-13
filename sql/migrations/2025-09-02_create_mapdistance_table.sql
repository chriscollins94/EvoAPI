-- Create MapDistance table for caching Google Maps Distance Matrix API results
-- This table caches travel distance and time data to reduce API costs and improve performance

IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='MapDistance' AND xtype='U')
BEGIN
    CREATE TABLE dbo.MapDistance (
        md_id int IDENTITY(1,1) PRIMARY KEY,
        md_address1 nvarchar(500) NOT NULL,           -- Origin address
        md_address2 nvarchar(500) NOT NULL,           -- Destination address
        md_distance_miles decimal(10,2) NULL,         -- Distance in miles
        md_distance_meters int NULL,                  -- Distance in meters
        md_distance_text nvarchar(50) NULL,           -- Formatted distance text (e.g., "5.2 mi")
        md_traveltime_minutes int NULL,               -- Travel time in minutes (no traffic)
        md_traveltime_seconds int NULL,               -- Travel time in seconds (no traffic)
        md_traveltime_text nvarchar(50) NULL,         -- Formatted travel time text (e.g., "8 mins")
        md_traveltime_traffic_minutes int NULL,       -- Travel time in minutes (with traffic)
        md_traveltime_traffic_seconds int NULL,       -- Travel time in seconds (with traffic)
        md_traveltime_traffic_text nvarchar(50) NULL, -- Formatted traffic time text (e.g., "12 mins")
        md_created_date datetime2 DEFAULT GETUTCDATE(), -- When this record was created
        md_last_updated datetime2 DEFAULT GETUTCDATE()  -- When this record was last updated
    );

    -- Create index for efficient lookups on address pairs
    CREATE INDEX IX_MapDistance_AddressPair ON dbo.MapDistance (md_address1, md_address2);
    
    -- Create index for cleanup/maintenance queries
    CREATE INDEX IX_MapDistance_CreatedDate ON dbo.MapDistance (md_created_date);
    
    PRINT 'MapDistance table created successfully';
END
ELSE
BEGIN
    PRINT 'MapDistance table already exists';
END
