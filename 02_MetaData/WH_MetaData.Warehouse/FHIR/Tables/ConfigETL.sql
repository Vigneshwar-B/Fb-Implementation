CREATE TABLE [FHIR].[ConfigETL] (
    [Id]               INT           NOT NULL,
    [TableName]        VARCHAR (100) NOT NULL,
    [SourceAPI]        VARCHAR (500) NOT NULL,
    [BronzeSchemaName] VARCHAR (50)  NOT NULL,
    [BronzeTableName]  VARCHAR (100) NOT NULL,
    [SilverSchemaName] VARCHAR (50)  NOT NULL,
    [SilverTableName]  VARCHAR (100) NOT NULL,
    [IsFullLoad]       BIT           NOT NULL,
    [WaterMarkField]   VARCHAR (100) NULL,
    [WaterMarkValue]   DATETIME2 (6) NULL,
    [IsActive]         BIT           NOT NULL
);


GO