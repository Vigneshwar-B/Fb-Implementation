CREATE   PROC [FHIR].[usp_UpdateWaterMark]
    @LakehouseName VARCHAR(128) = 'LH_Silver'      -- Silver Lakehouse name
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @union NVARCHAR(4000),
            @sql   NVARCHAR(4000);

    -- 1. Build one MAX() query per active table (local metadata query - variables allowed here)
    SELECT @union = STRING_AGG(
               CAST(N'SELECT ' + QUOTENAME(TableName, '''') + N' AS TableName, '
                  + N'MAX(CAST(' + QUOTENAME(WaterMarkField) + N' AS DATETIME2(6))) AS MaxWM '
                  + N'FROM ' + QUOTENAME(@LakehouseName) + N'.' + QUOTENAME(SilverSchemaName)
                  + N'.' + QUOTENAME(SilverTableName) AS NVARCHAR(4000)),
               N' UNION ALL ')
    FROM FHIR.ConfigETL
    WHERE IsActive = 1
      AND WaterMarkField IS NOT NULL;

    IF @union IS NULL
        THROW 50001, 'No active FHIR.ConfigETL rows with a WaterMarkField.', 1;

    -- 2. One set-based UPDATE; the Lakehouse part contains no variables, only literals
    SET @sql = N'UPDATE c
                 SET c.WaterMarkValue = s.MaxWM
                 FROM FHIR.ConfigETL AS c
                 INNER JOIN (' + @union + N') AS s
                         ON c.TableName = s.TableName
                 WHERE c.IsActive = 1
                   AND s.MaxWM IS NOT NULL
                   AND (c.WaterMarkValue IS NULL OR s.MaxWM > c.WaterMarkValue);';

    EXEC sp_executesql @sql;

    SELECT Id, TableName, WaterMarkField, WaterMarkValue
    FROM FHIR.ConfigETL
    WHERE IsActive = 1
    ORDER BY Id;
END;

GO