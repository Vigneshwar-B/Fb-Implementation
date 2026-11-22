CREATE TABLE [Log].[ETLBatchBronzeLogDetails] (
    [BatchId]           BIGINT        NULL,
    [TableId]           BIGINT        NULL,
    [SchemaName]        VARCHAR (MAX) NULL,
    [TableName]         VARCHAR (MAX) NULL,
    [StartTime]         DATETIME2 (6) NULL,
    [EndTime]           DATETIME2 (6) NULL,
    [ExtractedRowCount] BIGINT        NULL,
    [ErrorMessage]      VARCHAR (MAX) NULL,
    [Status]            VARCHAR (MAX) NULL,
    [SourceName]        VARCHAR (MAX) NULL
);


GO