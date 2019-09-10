USE [IERG]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE VIEW [history].[PERSON__INSTITUTIONS_ATTEND] AS
SELECT [ID]
       
, CAST(LTRIM(RTRIM(CA1.Item)) AS VARCHAR(7)) AS [PERSON.INSTITUTIONS.ATTEND]
     , CA1.ItemNumber AS ItemNumber
     , EffectiveDatetime
  FROM [history].[PERSON]
  
 CROSS APPLY dbo.DelimitedSplit8K([PERSON.INSTITUTIONS.ATTEND],', ') CA1
 WHERE COALESCE([PERSON.INSTITUTIONS.ATTEND],'') != ''
       
GO
