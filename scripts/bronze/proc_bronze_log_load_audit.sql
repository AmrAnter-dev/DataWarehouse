/*====================================================================
Object Name : bronze.log_load_audit
Object Type : Stored Procedure
Project     :  DataWarehouse
Layer       : Bronze (Operational Metadata Layer)

Author      : Amr Anter
Created On  : 2026-04-26
Version     : 1.0

Purpose:
    Writes load execution metadata into bronze.load_audit to support
    batch monitoring, lineage, observability, and troubleshooting.

Description:
    This procedure logs execution details for ingestion processes,
    including:
      - Batch identifier
      - Table processed
      - Load start and end timestamps
      - Row counts loaded
      - Execution status (SUCCESS / FAILED)
      - Error messages when applicable

Parameters:
    @batch_id         Unique identifier for ETL batch execution
    @table_name       Target table processed
    @load_start_time  Load process start timestamp
    @load_end_time    Load process completion timestamp
    @rows_loaded      Number of rows loaded
    @load_status      Execution status
    @error_message    Error details if load failed

Dependencies:
    - Table: bronze.load_audit

Usage Example:
    EXEC bronze.log_load_audit
         @batch_id = 1001,
         @table_name = 'crm_cust_info',
         @load_start_time = SYSDATETIME(),
         @load_end_time = SYSDATETIME(),
         @rows_loaded = 15000,
         @load_status = 'SUCCESS',
         @error_message = NULL;

Notes:
    - Intended to be called by Bronze ingestion procedures
    - Supports operational auditing and restartability tracking

Change History
----------------------------------------------------------------------
Version   Date         Author        Description
1.0       2026-04-26   Amr Anter     Initial creation
====================================================================*/

CREATE OR ALTER PROCEDURE bronze.log_load_audit
(
   @batch_id BIGINT,
   @table_name VARCHAR(100),
   @load_start_time DATETIME2 = NULL,
   @load_end_time DATETIME2 = NULL,
   @rows_loaded BIGINT = NULL,
   @load_status VARCHAR(20),
   @error_message NVARCHAR(4000)=NULL
)
AS
BEGIN
   SET NOCOUNT ON;

   INSERT INTO bronze.load_audit
   (
      batch_id,
      table_name,
      load_start_time,
      load_end_time,
      rows_loaded,
      load_status,
      error_message
   )
   VALUES
   (
      @batch_id,
      @table_name,
      @load_start_time,
      @load_end_time,
      @rows_loaded,
      @load_status,
      @error_message
   );

END;
GO

CREATE OR ALTER PROCEDURE bronze.log_load_audit
(
   @batch_id BIGINT,
   @table_name VARCHAR(100),
   @load_start_time DATETIME2 = NULL,
   @load_end_time DATETIME2 = NULL,
   @rows_loaded BIGINT = NULL,
   @load_status VARCHAR(20),
   @error_message NVARCHAR(4000)=NULL
)
AS
BEGIN
   SET NOCOUNT ON;

   INSERT INTO bronze.load_audit
   (
      batch_id,
      table_name,
      load_start_time,
      load_end_time,
      rows_loaded,
      load_status,
      error_message
   )
   VALUES
   (
      @batch_id,
      @table_name,
      @load_start_time,
      @load_end_time,
      @rows_loaded,
      @load_status,
      @error_message
   );
END;
GO
