CREATE TABLE [Log].[FHIRApiCallLog] (
    [BatchId]     BIGINT        NULL,
    [TableId]     BIGINT        NULL,
    [TableName]   VARCHAR (MAX) NULL,
    [PageNumber]  INT           NULL,
    [ApiUrl]      VARCHAR (MAX) NULL,
    [HttpStatus]  INT           NULL,
    [EntryCount]  INT           NULL,
    [ApiCalledAt] DATETIME2 (6) NULL,
    [RawSavedAt]  DATETIME2 (6) NULL,
    [RawFilePath] VARCHAR (MAX) NULL,
    [HasNextPage] BIT           NULL
);


GO