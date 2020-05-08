CREATE VIEW [${ViewSchema}].[${ViewName}] AS
  SELECT ${TableColumns} 
  FROM [${ViewSchema}].[${TableName}]
  WHERE CurrentFlag = 'Y'