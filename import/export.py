import sys
#import yaml
import urllib
import pyodbc
import pandas as pd
import numpy as np
import os
import shutil
from os import path
import glob
from string import Template
import sqlalchemy
from sqlalchemy import exc
import zipfile
import re
#import smtplib
#from email.header    import Header
#from email.mime.text import MIMEText
#from getpass         import getpass
#import ssl
import numpy as np
from loguru import logger

import functools

#sql_schema = cfg['sql']['schema']
#sql_schema_history = cfg['sql']['schema_history']

# executeSQL_INSERT() - attempts to create SQL code from csv files and push it to the SQL server
@logger.catch
def executeSQL_INSERT( engine, df, sqlName, 
                       dataTypesDict, dataTypeMVDict, 
                       svr_tables_input, svr_tables_history, svr_columns, 
                       logger, cfg ):

    sql_schema = cfg['sql']['schema']
    sql_schema_history = cfg['sql']['schema_history']
    
    logger.debug("Fix all non-string columns, replace blanks with NAs which become NULLs in DB, and remove commas")
    nonstring_columns = [key for key, value in dataTypesDict.items() if type(dataTypesDict[key]) != sqlalchemy.sql.sqltypes.String]
    df[nonstring_columns] = df[nonstring_columns].replace({'':np.nan, ',':''}, regex=True) # 2018-06-18 C DMO

    # Attempt to push the new data to the existing SQL Table.
    try:
        new_columns = list(set(list(df.columns)) - set(list(svr_columns["COLUMN_NAME"])))
        if list(svr_columns["COLUMN_NAME"]) != [] and new_columns != []: 
            logger.debug("Attempt to add new columns to table in SQL Server")
            executeSQLAppend(engine, df, sqlName, dataTypesDict, logger, sql_schema, cfg)
    except:
        logger.exception("Unknown error in executeSQLAppend: ",sys.exc_info()[0])
        # Write out the MERGE SQL code that failed
        ef = open(f"MergeError_{sqlName}.sql", 'w')
        #ef.write(result)
        ef.close()
        raise
#        pass

    try:
        logger.debug( "Push {0} data to SQL schema {1}".format( sqlName, sql_schema ) )

        df.to_sql(sqlName, engine, schema=sql_schema, if_exists='append',
                  index=False, index_label=None, chunksize=None, dtype=dataTypesDict)

    except (exc.SQLAlchemyError, exc.DBAPIError, exc.ProgrammingError) as er:
        # This is a temporary line. Needs to be for ProgrammingError ONLY!
        logger.debug( f"Error Msg: {str(er.orig.args[1])}" )
        logger.exception( f"Error in File:\t{sqlName}\n\n Error: {er}\n DataTypes: {dataTypesDict}\n\n" )
        raise
    except:
        logger.exception( f"Unknown error in executeSQL_INSERT: {sys.exc_info()[0]}" )
        raise    

# executeSQL_MERGE() - Creates SQL Code based on current Table/Dataframe by using a Template then pushes to History
@logger.catch
def executeSQL_MERGE( engine, df, sqlName, 
                      keyListDict, 
                      dataTypesDict, dataTypeMVDict, 
                      elementAssocTypesDict, elementAssocNamesDict,
                      svr_tables_input, svr_tables_history, svr_columns, 
                      logger, cfg ):
    
    sql_schema = cfg['sql']['schema']
    sql_schema_history = cfg['sql']['schema_history']

    # Get a list of all the keys for this table
    TableKeys = list(keyListDict.keys()) 

    logger.debug( "Create blank dataframe for output\n" )    

    # Create and push a blankFrame with no data to SQL Database for History Tables
    blankFrame = pd.DataFrame(columns=df.columns)
    # History does not use DataDatetime, so delete that
    del blankFrame['DataDatetime']

    # Get a list of all the columns in this table. 
    # This is needed for the template below.
    TableColumns = list(blankFrame.columns) 
    
    # Add three History columns to blankFrame
    blankFrame['EffectiveDatetime'] = ""
    blankFrame['ExpirationDatetime'] = ""
    blankFrame['CurrentFlag'] = ""
    
    # Add three History column types to dataTypesDict
    blankTyper = dataTypesDict
    blankTyper['EffectiveDatetime'] = sqlalchemy.types.DateTime()
    blankTyper['ExpirationDatetime'] = sqlalchemy.types.DateTime()
    blankTyper['CurrentFlag'] = sqlalchemy.types.String(1)

    flds_columns = {'TableSchema' : "'" + cfg['sql']['schema_history'] + "'", 
                    'TableName'   : "'" + sqlName + "'" }

    table_columns_filein = open(cfg['sql']['table_column_names'],"r")
    table_columns_src = Template( table_columns_filein.read() )

    filein = open(cfg['sql']['drop_view'],"r")
    dropViewTemplate = Template( filein.read() )
    filein.close()
    # Read in template file to create a view
    filein = open(cfg['sql']['view_create'],"r")
    createViewTemplate = Template( filein.read() )
    filein.close()

    if not sqlName in svr_tables_history:
        # This will create the history table 
        try:
            # Send the blank dataframe which creates missing tables
            blankFrame.to_sql(sqlName, engine, schema=sql_schema_history, if_exists='fail',
                                index=False, index_label=None, chunksize=None, dtype=blankTyper)

        except (exc.SQLAlchemyError, exc.DBAPIError, exc.ProgrammingError) as er:
            logger.exception( f"Error in File: \t{sqlName}\n\n Error: {er}\n\n" )
            raise

        except er:
            logger.exception( f"Error: {er}" )
            raise

        else:

            sql_col_query = table_columns_src.substitute(flds_columns)
            
            svr_columns_history = pd.read_sql(sql_col_query, engine)
            svr_columns_history.reset_index(inplace=True)
            svr_columns_history.set_index("COLUMN_NAME",inplace=True)
            svr_columns_history_dict = svr_columns_history.to_dict(orient="index")

            # Read in template file to alter the table for the keys
            fileinKeysAlter = open(cfg['sql']['alter_table_key_column'],"r")
            srcKeysAlter = Template( fileinKeysAlter.read() )
            fileinKeysAlter.close()

            # If it is the first time the history table is created then create a view as well
            try:
                logger.debug( "--Creating History Keys" )

                # Set keys to not null
                for key in keyListDict:

                    flds_keys = {'TableSchema'   : sql_schema_history,
                                 'TableName'     : sqlName,
                                 'TableKey'      : key,
                                 'TableKey_Type' : svr_columns_history_dict[key]["DATA_TYPE_TEXT"]
                                }
                    resultKeysAlter = srcKeysAlter.substitute(flds_keys)
                    engine.execute(resultKeysAlter)

                flds_keys = {'TableSchema'   : sql_schema_history,
                             'TableName'     : sqlName,
                             'TableKey'      : "EffectiveDatetime",
                             'TableKey_Type' : svr_columns_history_dict["EffectiveDatetime"]["DATA_TYPE_TEXT"]
                            }
                resultKeysAlter = srcKeysAlter.substitute(flds_keys)
                engine.execute(resultKeysAlter)

                # Read in template file to create the actual primary key
                fileinKeys = open(cfg['sql']['alter_table_keys'],"r")
                srcKeys = Template( fileinKeys.read() )
                fileinKeys.close()

                flds_pk = {'TableSchema_DEST' : sql_schema_history,
                           'TableName'        : sqlName,
                           'pkName'           : 'pk_' + sqlName,
                           'primaryKeys'      : ', '.join("[{0}]".format(c) for c in keyListDict)
                          }
                
                resultKeys = srcKeys.substitute(flds_pk)
                engine.execute(resultKeys)

            except:
                logger.debug("-Error Creating History Keys for SQL Table" )
                logger.debug(resultKeysAlter)
                logger.debug(resultKeys)
                raise

    else:
        sql_col_query = table_columns_src.substitute(flds_columns)
        svr_columns_history = pd.read_sql(sql_col_query, engine)
        svr_columns_history.reset_index(inplace=True)
        svr_columns_history.set_index("COLUMN_NAME",inplace=True)
        svr_columns_history_dict = svr_columns_history.to_dict(orient="index")


    # We are treating all non-key columns as Type 2 SCD at this time (20170721)
    TableColumns2 = TableColumns 
    TableDefaultDate = min(df['DataDatetime'])
    
    flds = {'TableSchema_SRC'      : sql_schema, 
            'TableSchema_DEST'     : sql_schema_history,
            'TableName'            : sqlName,
            'TableKeys'            : ', '.join("[{0}]".format(k) for k in TableKeys),
            'TableKeys_wTypes'     : ', '.join("{0} [{1}]".format(k,svr_columns_history_dict[k]["DATA_TYPE_TEXT"]) for k in TableKeys),
            'TableColumns'         : ', '.join("[{0}]".format(c) for c in TableColumns),
            'TableKeys_CMP'        : ' AND '.join("DEST.[{0}] = SRC.[{0}]".format(k) for k in TableKeys),
            'TableKeys_SRC'        : ', '.join("SRC.[{0}]".format(k) for k in TableKeys),
            'TableColumns_SRC'     : ', '.join("SRC.[{0}]".format(c) for c in TableColumns),
            'TableColumns1_SRC'    : ', '.join([]), # There are no Type 1 SCD at this time
            'TableColumns1_DEST'   : ', '.join([]), # There are no Type 1 SCD at this time
            'TableColumns1_UPDATE' : ', '.join([]), # There are no Type 1 SCD at this time
            'TableColumns2_SRC'    : ', '.join("SRC.[{0}]".format(c) for c in TableColumns2),
            'TableColumns2_DEST'   : ', '.join("DEST.[{0}]".format(c) for c in TableColumns2),
            'TableDefaultDate'     : TableDefaultDate,
            'ViewSchema'           : sql_schema_history,
            'ViewName'             : sqlName + '_Current',
            'ViewName2'            : sqlName + '_test',
            'pkName'               : 'pk_' + sqlName,
            'primaryKeys'          : ', '.join("[{0}]".format(c) for c in keyListDict),
            'ViewColumns'          : ', '.join("[{0}]".format(c) for c in TableColumns)
    }

    try:
        dropViewSQL = dropViewTemplate.substitute(flds)
        createViewSQL = createViewTemplate.substitute(flds)
        #dropView = f"DROP VIEW IF EXISTS {sql_schema_history}.{flds['viewName']}"

        logger.debug(f"--Creating History View {sql_schema_history}.{flds['ViewName']} (dropping if exists)")
        #drop sql view if exits
        engine.execute(dropViewSQL)

        logger.debug(f"View {sql_schema_history}.{flds['ViewName']} DDL:")
        logger.debug(createViewSQL)

        #create new sql view
        engine.execute(createViewSQL)

    except:
        logger.exception ("-Error Creating View of SQL Table" )
        logger.debug(dropViewSQL)
        raise

    # Read in template file to create a view
    filein = open(cfg['sql']['view2_cast'],"r")
    view2CastTemplate = Template( filein.read() )
    filein.close()
    # Read in template file to create a view
    filein = open(cfg['sql']['view2_crossapply'],"r")
    view2CrossApplyTemplate = Template( filein.read() )
    filein.close()
    # Read in template file to create a view
    filein = open(cfg['sql']['view2_whereand'],"r")
    view2WhereAndTemplate = Template( filein.read() )
    filein.close()

    try:
        # !!!!
        # !!!! This does not work on subsequent tables after a fail
        # !!!! DMO 2017-08-30 Should be fixed now, was deleting from global value

        elementAssocNamesSet = set( val for dic in [elementAssocNamesDict] for val in dic.values())
        for elementAssocName in elementAssocNamesSet: 
            logger.debug(f"Processing association {elementAssocName}")
            associationKey = ''
            lastelement = ''
            view2CastSQL = ''
            view2CrossApplySQL = ''
            view2WhereAndSQL = ''
            #CastStr = ''
            #CrossApplyStr = ''
            #WhereAndStr = ''
            counter = 0

            for elementAssocIndex, elementAssocKey in enumerate(list(elementAssocNamesDict)):
                if (elementAssocNamesDict[elementAssocKey] == elementAssocName):
                    logger.debug(f"Found element of association: {elementAssocKey}")
                    counter += 1

                    flds_view2 = { 'ItemType'              : dataTypeMVDict[elementAssocKey],
                                   'Counter'               : counter,
                                   'ElementAssociationKey' : elementAssocKey
                                 }
                    # associationGroup = elementAssocNamesDict[]

                    view2CastSQL += view2CastTemplate.substitute(flds_view2)
                    view2CrossApplySQL += view2CrossApplyTemplate.substitute(flds_view2)
                    #CastStr += f"\n, CAST(LTRIM(RTRIM(CA{counter}.Item)) AS {dataTypeMVDict[elementAssocKey]}) AS [{elementAssocKey}]"
                    #CrossApplyStr += f"\n CROSS APPLY util.DelimitedSplit8K([{elementAssocKey}],\', \') CA{counter}"

                    if(counter > 1):
                        logger.debug(f"Counter > 1, adding comparison of 1 and {counter}")
                        view2WhereAndSQL += view2WhereAndTemplate.substitute(flds_view2)
                        #WhereAndStr += f"AND CA1.ItemNumber=CA{counter}.ItemNumber\n"

                    # If single MV is not a key, need to force it to be one
                    lastelement = elementAssocKey

                    if (elementAssocTypesDict[elementAssocKey] == 'K'):
                        associationKey = elementAssocKey
                        view_Name = elementAssocNamesDict[elementAssocKey]
                        logger.debug(f"Add key {elementAssocKey} to association {view_Name.replace('.', '_')}")

            if (associationKey == ''):
                if (lastelement != ''):
                    associationKey = lastelement
                    view_Name = elementAssocNamesDict[associationKey]
                    logger.debug(f"Setting associationKey to {lastelement} for association {view_Name.replace('.', '_')}")

                else:
                    logger.debug(f"No associationKey to set for association {elementAssocName}")
                    raise(AssertionError)

            view2_Str = f"{sqlName}__{view_Name.replace('.', '_')}"

            flds2 = {
                    'TableName'       : sqlName,
                    'ViewSchema'      : sql_schema_history,
                    'ViewName'        : view2_Str,
                    'primaryKeys'     : ', '.join(f"[{c}]" for c in keyListDict),
                    'CastStr'         : view2CastSQL,
                    'CrossApplyStr'   : view2CrossApplySQL,
                    'WhereAndStr'     : view2WhereAndSQL,
                    'associationKeys' : associationKey
            }

            filein2 = open(cfg['sql']['view2_create'],"r")
            src2 = Template( filein2.read() )
            result2 = src2.substitute(flds2)

            logger.debug(f"View {view2_Str} DDL:")
            logger.debug(result2)

            dropViewSQL = dropViewTemplate.substitute(flds2)
            createViewSQL = createViewTemplate.substitute(flds2)
            #dropView = f"DROP VIEW IF EXISTS {sql_schema_history}.{view2_Str}"
            logger.debug(f"--Creating History View2 {sql_schema_history}.{view2_Str} (dropping if exists)")
            #drop sql view if exits
            engine.execute(dropViewSQL)
            engine.execute(result2)

    except:
        logger.exception('Creating View2 failed')
        ef = open(f"View2Error_{view2_Str}.sql", 'w')
        ef.write(result2)
        ef.close()
        raise

    try:
        logger.debug("..creating History Table")
        blankFrame.to_sql(sqlName, engine, schema=sql_schema_history, if_exists='append',
                         index=False, index_label=None, chunksize=None, dtype=blankTyper)

    except (exc.SQLAlchemyError, exc.DBAPIError, exc.ProgrammingError) as er:
        logger.error(f"-creating SQL from df - skippeSd SQL Alchemy ERROR [{+str(er.args[0])}]")
        logger.exception(f"Error in File: \t{sqlName}\n\n Error: {er}\n DataTypes: {dataTypesDict}\n\n" )
        raise

    logger.debug("..wrote to table")

    #Attempt to push the Column data to the existing SQL Table if there are new Columns to be added.
    try:
        executeSQLAppend(engine, blankFrame, sqlName, dataTypesDict, logger, sql_schema_history, cfg)
        
    except:
        logger.exception('append didnt work')
        raise

    flds = {'TableSchema_SRC'      : sql_schema, 
            'TableSchema_DEST'     : sql_schema_history,
            'TableName'            : sqlName,
            'TableKeys'            : ', '.join("[{0}]".format(k) for k in TableKeys),
            'TableKeys_wTypes'     : ', '.join("{0} [{1}]".format(k,svr_columns_history_dict[k]["DATA_TYPE_TEXT"]) for k in TableKeys),
            'TableColumns'         : ', '.join("[{0}]".format(c) for c in TableColumns),
            'TableKeys_CMP'        : ' AND '.join("DEST.[{0}] = SRC.[{0}]".format(k) for k in TableKeys),
            'TableKeys_SRC'        : ', '.join("SRC.[{0}]".format(k) for k in TableKeys),
            'TableColumns_SRC'     : ', '.join("SRC.[{0}]".format(c) for c in TableColumns),
            'TableColumns1_SRC'    : ', '.join([]), # There are no Type 1 SCD at this time
            'TableColumns1_DEST'   : ', '.join([]), # There are no Type 1 SCD at this time
            'TableColumns1_UPDATE' : ', '.join([]), # There are no Type 1 SCD at this time
            'TableColumns2_SRC'    : ', '.join("SRC.[{0}]".format(c) for c in TableColumns2),
            'TableColumns2_DEST'   : ', '.join("DEST.[{0}]".format(c) for c in TableColumns2),
            'TableDefaultDate'     : TableDefaultDate,
            'viewSchema'           : sql_schema_history,
            'viewName'             : sqlName + '_Current',
            'viewName2'            : sqlName + '_test',
            'pkName'               : 'pk_' + sqlName,
            'primaryKeys'          : ', '.join("[{0}]".format(c) for c in keyListDict),
            'viewColumns'          : ', '.join("[{0}]".format(c) for c in TableColumns)
    }

    filein = open(cfg['sql']['merge_scd2'],"r")
    src = Template( filein.read() )
    result = src.substitute(flds)

    # Attempt to execute generated SQL MERGE code
    try:
        logger.debug("...executing sql command")
        rtn = engine.execute(result)

    except (exc.SQLAlchemyError, exc.DBAPIError, exc.ProgrammingError, pyodbc.Error, pyodbc.ProgrammingError) as er:
        logger.error(f"---executing sql command - skipped SQL ERROR [{str(er.args[0])}]")
        logger.exception(f"Error in File: \t {sqlName}\n\n Error: {er}\n\n\n")
        ef = open(f"MergeError_{sqlName}.sql", 'w')
        ef.write(result)
        ef.close()
        raise

    # Deletes Tables from SQL Database if coppied to the history table
    try:
        logger.debug("...executing delete command")
        filein = open(cfg['sql']['delete_table_data'],"r")
        deleteDataTemplate = Template( filein.read() )
        deleteDataSQL = deleteDataTemplate.substitute(flds)

        #rtn = engine.execute(f"DELETE FROM [{sql_schema}].[{sqlName}]\n\nCOMMIT")
        rtn = engine.execute(deleteDataSQL)

    except (exc.SQLAlchemyError, exc.DBAPIError, exc.ProgrammingError, pyodbc.Error, pyodbc.ProgrammingError) as er:
        logger.error(f"---executing DELETE command - skipped SQL ERROR [{str(er.args[0])}]")
        logger.exception(f"Error in File: \t {sqlName}\n\n Error: {er}\n\n\n")
        raise
    logger.debug('....wrote to history')


# executeSQL_UPDATE() - calls both executeSQL_INSERT and executeSQL_MERGE in attempt to update the SQL Tables 
@logger.catch
def executeSQL_UPDATE( engine, df, sqlName, 
                       keyListDict, 
                       dataTypesDict, dataTypeMVDict, 
                       elementAssocTypesDict, elementAssocNamesDict, 
                       svr_tables_input, svr_tables_history, svr_columns,
                       logger, cfg ):
    try:
        executeSQL_INSERT( engine, df, sqlName, 
                           dataTypesDict, dataTypeMVDict, 
                           svr_tables_input, svr_tables_history, svr_columns, 
                           logger, cfg )
    except:
        logger.exception('XXXXXXX failed on executeSQL_INSERT XXXXXXX')
        raise
    try:
        executeSQL_MERGE( engine, df, sqlName, 
                          keyListDict, 
                          dataTypesDict, dataTypeMVDict,
                          elementAssocTypesDict, elementAssocNamesDict, 
                          svr_tables_input, svr_tables_history, svr_columns, 
                          logger, cfg )
        pass
    except:
        logger.exception('XXXXXXX failed on executeSQL_MERGE XXXXXXX')
        raise


# ig_f() - Used to find files to be ignored when copying the dir tree
def ig_f(dir, files):
    return [f for f in files if os.path.isfile(os.path.join(dir, f))]


# archive() - Archives files after they are processed
@logger.catch
def archive(df, subdir, file, exportPath, archivePath, logger, cfg, diffs = True, createInitial = True):
    # Create the path in the archive based on the location of the CSV
    if not os.path.isdir(os.path.join(archivePath, subdir)):
        shutil.copytree(os.path.join(exportPath, subdir),os.path.join(archivePath,subdir), ignore=ig_f)

    if cfg['ccdw']['archive_type'] == 'zip':
        if not os.path.isfile(os.path.join(archivePath, subdir, file)):
            try:
                # Create a zip'd version of the CSV
                zFi = zipfile.ZipFile(os.path.join(exportPath,subdir,(file[:-4]+'.zip')), 'w', zipfile.ZIP_DEFLATED)
                zFi.write(os.path.join(exportPath, subdir, file), file)
                zFi.close()

                # Move the zip file to the archive location
                shutil.move(os.path.join(exportPath, subdir, (file[:-4]+'.zip')), os.path.join(archivePath, subdir, (file[:-4]+'.zip')))

                # Remove the CSV file from the export folder
                os.remove(os.path.join(exportPath, subdir, file)) # comment this out if you want to keep files
            except:
                raise
    else:
        if cfg['ccdw']['archive_type'] == 'move':
            archive_filelist = sorted(glob.iglob(os.path.join(archivePath, subdir, subdir + '_Initial.csv')), 
                                      key=os.path.getctime)
            if (len(archive_filelist) == 0):
                logger.debug("INITALARCHIVE: Creating...")
                df.to_csv( os.path.join(archivePath, subdir, subdir + '_Initial.csv'), 
                           index = False, date_format="%Y-%m-%dT%H:%M:%SZ" )

            if diffs:
                shutil.move(os.path.join(exportPath, subdir, file), os.path.join(archivePath, subdir, file))
            else:
                # Move the file to the archive location
                shutil.move(os.path.join(exportPath, subdir, file), os.path.join(archivePath, subdir, subdir + '.csv'))
                df.to_csv( os.path.join(archivePath, subdir, file), index = False, date_format="%Y-%m-%dT%H:%M:%SZ" )


# engine() - creates an engine to be used to interact with the SQL Server
@logger.catch
def engine( driver, server, db, schema ):
    conn_details =  f"""
      DRIVER={{{driver}}};SERVER={server};DATABASE={db};SCHEMA={schema};Trusted_Connection=Yes;
    """

    params = urllib.parse.quote_plus(conn_details)

    engine = sqlalchemy.create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
    return engine



# executeSQLAppend() - Attempts to execute an Append statement to the SQL Database with new Columns
@logger.catch
def executeSQLAppend(engine, df, sqlName, dataTypesDict, logger, schema, cfg):
    sql_schema = cfg['sql']['schema']
    sql_schema_history = cfg['sql']['schema_history']

    TableColumns= list(df.columns) 

    #newColumnCheck = False
    logger.debug('_______________________________________________________________________')
    #create SQL string and read matching Table on the server
    sqlStrings = f"SELECT * FROM {schema}.{sqlName}"
    sqlRead = pd.read_sql(sqlStrings, engine)
    logger.debug('--Diff:')
    existingColumns = list(sqlRead.columns)
    newnames = list(df[df.columns.difference(existingColumns)].columns)
    logger.debug(f"Newnames = {newnames}")

    if not newnames:
        logger.debug("No new columns")
        return()

    #attemp to create a dataframe and string of columns that need to be added to SQL Server
    try:
        updateList = list(set(list(sqlRead)).symmetric_difference(set(list(df))))
        logger.debug(f"new columns: {updateList}")
        updateFrame = pd.DataFrame(columns=updateList)
        updateColumns= list(updateFrame.columns)   # "_wStatus" if wStatus else ""
        updateColumns1 = ',\n\t'.join(f"[{c}] {dataTypesDict[c]}" for c in reversed(updateColumns))
        # Replace VARCHAR with VARCHAR(MAX)
        # Regular Expression: VARCHAR,|VARCHAR$  -> VARCHAR(MAX)
        updateColumns1 = re.sub('VARCHAR(,)|VARCHAR($)','VARCHAR(MAX)\\1',updateColumns1)
    except:
        logger.exception('ERROR!!!!')

    logger.debug(f"UpdateColumns1: {updateColumns1}")
    
    #create SQL File based on tempalte to ALTER current SQL Table and append new Columns
    flds = {'TableSchema'          : schema, 
            'TableName'            : sqlName,
            'TableColumns'         : ', '.join(f"[{c}]" for c in TableColumns),
            'updateColumns'        : updateColumns1,
            'viewName'             : sqlName + '_Current',
            'viewSchema'           : sql_schema_history
        }
    filein = open(cfg['sql']['alter_table_column'],"r")
    src = Template( filein.read() )
    result = src.substitute(flds)
    logger.debug('_______________________________________________________________________')
    
    #if there are added Columns attempt to push them to the SQL Table
    try:
        if(updateColumns):
            engine.execute(result)
            #newColumnCheck = True

    except (exc.SQLAlchemyError, exc.DBAPIError, exc.ProgrammingError) as er:
        logger.error(f"-Error Updating Columns in SQL Table - [{str(er.args[0])}]")
        logger.exception(f"Error in File: \t {sqlName}\n\n Error:{er}\n\n")
        ef = open(f"MergeError_{sqlName}.sql", 'w')
        ef.write(result)
        ef.close()
        raise

    if (updateColumns):
        filein = open(cfg['sql']['create_view3'],"r")
        src = Template( filein.read() )
        result = src.substitute(flds)
        dropView = f"DROP VIEW IF EXISTS {flds['viewSchema'] + '.' + flds['viewName']}"
        try:
            logger.debug('----Creating Current View')
            #drop sql view if exits
            engine.execute(dropView)
            #create new sql view
            engine.execute(result)

        except (exc.SQLAlchemyError, exc.DBAPIError, exc.ProgrammingError) as er:
            logger.error("-Error Creating View of SQL Table - ["+str(er.args[0])+"]" )
            logger.exception("Error in Table: \t {sqlName}\n\n Error:{er}}\n\n")
            raise

# numericalSort() - Is used to properly sort files based on numbers correctly
@logger.catch
def numericalSort(value):
    numbers = re.compile(r'(\d+)')
    parts = numbers.split(value)
    parts[1::2] = map(int, parts[1::2])
    return parts

@logger.catch
def createDiff( cf, lf ):
    if (lf.shape[0] == 0) | (cf.shape[0] == 0):
        return( cf )
        
    # Old version is last one archived
    lf['version'] = "old"

    # New version is the current one being processed
    cf['version'] = "new"

    #Join all the data together and ignore indexes so it all gets added
    full_set = pd.concat([lf, cf],ignore_index=True)

    # Get all column names except 'version' defined above into col_names
    # col_names = full_set.column.names - 'version'
    col_names = full_set[full_set.columns.difference(["DataDatetime", "version"])].columns

    # Let's see what changes in the main columns we care about, keep only new records
    changes = full_set.drop_duplicates(subset=col_names, keep=False)    
    changes = changes[(changes["version"] == "new")]

    #Drop the temp columns - we don't need them now
    changes = changes.drop(['version'], axis=1)
    
    return( changes )
