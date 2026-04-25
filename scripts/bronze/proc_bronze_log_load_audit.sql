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