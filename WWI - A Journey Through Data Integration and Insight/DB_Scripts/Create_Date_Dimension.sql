-- Create the Date Dimension table
USE [WideWorldImportersPurchase]
GO

CREATE TABLE [Dimension].[Date](
    [Date] [date] NOT NULL,
    [Day Number] [int] NOT NULL,
	[Day] [nvarchar](10) NOT NULL,
	[Month] [nvarchar](10) NOT NULL,
	[Short Month] [nvarchar](3) NOT NULL,
    [Calendar Month Number] [int] NOT NULL,
    [Calendar Month Label] [nvarchar](20) NOT NULL,
    [Calendar Year] [int] NOT NULL,
    [Calendar Year Label] [nvarchar](10) NOT NULL,
    [Fiscal Month Number] [int] NOT NULL,
    [Fiscal Month Label] [nvarchar](20) NOT NULL,
    [Fiscal Year] [int] NOT NULL,
    [Fiscal Year Label] [nvarchar](10) NOT NULL,
    [ISO Week Number] [int] NOT NULL,
CONSTRAINT [PK_Dimension_Date] PRIMARY KEY CLUSTERED ([Date])
);

-- Populate the Date Dimension table
DECLARE @StartDate DATE = '2013-01-01';
DECLARE @EndDate DATE = '2016-12-31';
DECLARE @CurrentDate DATE = @StartDate;

WHILE @CurrentDate <= @EndDate
BEGIN
    INSERT INTO [Dimension].[Date] (
        [Date],
        [Day Number],
        [Day],
        [Month],
        [Short Month],
        [Calendar Month Number],
        [Calendar Month Label],
        [Calendar Year],
        [Calendar Year Label],
        [Fiscal Month Number],
        [Fiscal Month Label],
        [Fiscal Year],
        [Fiscal Year Label],
        [ISO Week Number]
    )
    VALUES (
        @CurrentDate,
        DATEPART(DAY, @CurrentDate),
        DATENAME(WEEKDAY, @CurrentDate),
        DATENAME(MONTH, @CurrentDate),
        LEFT(DATENAME(MONTH, @CurrentDate), 3),
        MONTH(@CurrentDate),
        CONCAT(DATENAME(MONTH, @CurrentDate), ' ', YEAR(@CurrentDate)),
        YEAR(@CurrentDate),
        CONCAT('CY ', YEAR(@CurrentDate)),
        CASE WHEN MONTH(@CurrentDate) >= 7 THEN MONTH(@CurrentDate) - 6 ELSE MONTH(@CurrentDate) + 6 END,
        CONCAT('FM ', CASE WHEN MONTH(@CurrentDate) >= 7 THEN MONTH(@CurrentDate) - 6 ELSE MONTH(@CurrentDate) + 6 END),
        CASE WHEN MONTH(@CurrentDate) >= 7 THEN YEAR(@CurrentDate) + 1 ELSE YEAR(@CurrentDate) END,
        CONCAT('FY ', CASE WHEN MONTH(@CurrentDate) >= 7 THEN YEAR(@CurrentDate) + 1 ELSE YEAR(@CurrentDate) END),
        DATEPART(ISO_WEEK, @CurrentDate)
    );

    SET @CurrentDate = DATEADD(DAY, 1, @CurrentDate);
END;

-- Confirm that data has been inserted correctly
SELECT TOP 10 * FROM [Dimension].[Date];
