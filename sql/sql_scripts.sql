-- ============================================================
-- FILE: create_source_table.sql
-- Description: Creates the main source table that stores raw
--              car sales data ingested from GitHub CSV via ADF
-- Database: Azure SQL Database (ashwarycarsaleserver)
-- ============================================================

IF NOT EXISTS (
    SELECT * FROM sysobjects WHERE name='source_cars_data' AND xtype='U'
)
BEGIN
    CREATE TABLE [dbo].[source_cars_data] (
        [Branch_ID]   VARCHAR(50)  NOT NULL,
        [Dealer_ID]   VARCHAR(50)  NOT NULL,
        [Model_ID]    VARCHAR(50)  NOT NULL,
        [Revenue]     BIGINT       NOT NULL,
        [Units_Sold]  BIGINT       NOT NULL,
        [Date_Id]     VARCHAR(20)  NOT NULL,
        [Day]         INT          NOT NULL,
        [Month]       INT          NOT NULL,
        [Year]        INT          NOT NULL,
        [Branch_Name] VARCHAR(100) NULL,
        [Dealer_Name] VARCHAR(100) NULL
    );
    PRINT 'Table source_cars_data created successfully.';
END
ELSE
BEGIN
    PRINT 'Table source_cars_data already exists.';
END
GO


-- ============================================================
-- FILE: create_watermark_table.sql
-- Description: Creates the watermark table used by ADF to
--              track incremental load progress (last loaded date)
-- ============================================================

IF NOT EXISTS (
    SELECT * FROM sysobjects WHERE name='watermark_table' AND xtype='U'
)
BEGIN
    CREATE TABLE [dbo].[watermark_table] (
        [Last_load] VARCHAR(20) NOT NULL
    );

    -- Seed with a date before any data exists so first run loads everything
    INSERT INTO [dbo].[watermark_table] ([Last_load])
    VALUES ('1900-01-01');

    PRINT 'Table watermark_table created and seeded successfully.';
END
ELSE
BEGIN
    PRINT 'Table watermark_table already exists.';
END
GO


-- ============================================================
-- FILE: update_watermark_sp.sql
-- Description: Stored procedure called by ADF after each
--              successful incremental load to update the
--              watermark with the latest processed date
-- ============================================================

CREATE OR ALTER PROCEDURE [dbo].[UpdateWatermarkTAble]
    @LastLoad VARCHAR(20)
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE [dbo].[watermark_table]
    SET [Last_load] = @LastLoad;

    PRINT CONCAT('Watermark updated to: ', @LastLoad);
END
GO


-- ============================================================
-- VERIFICATION QUERIES (run after setup to confirm everything)
-- ============================================================

-- Check source table structure
SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'source_cars_data'
ORDER BY ORDINAL_POSITION;

-- Check watermark value
SELECT * FROM [dbo].[watermark_table];

-- Check record count after source_prep pipeline run
SELECT COUNT(*) AS total_records FROM [dbo].[source_cars_data];

-- Check date range in source data (used to validate incremental logic)
SELECT
    MIN(Date_Id) AS earliest_date,
    MAX(Date_Id) AS latest_date,
    COUNT(*)     AS total_records
FROM [dbo].[source_cars_data];
