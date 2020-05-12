import sys
import urllib
import pyodbc
import pandas as pd
import numpy as np
import os
from os import path
import glob
from string import Template
import sqlalchemy
from sqlalchemy import exc
import re
import copy
import numpy as np
from loguru import logger

class CCDW_Export:
    __cfg = None
    __meta = None
    __logger = None

    __export_path = ""
    __archive_path = ""

    engine = None
    svr_tables_input = {}
    svr_tables_history = {}
    sql_schema_input = ""
    sql_schema_history = ""

    tableColumnsNamesTemplate = ""
    dropViewTemplate = ""
    viewCreateTemplate = ""
    alterTableKeyColumnTemplate = ""
    alterTableKeysTemplate = ""
    alterTableTemplate = ""
    view2CastTemplate = ""
    view2CrossApplyTemplate = ""
    view2WhereAndTemplate = ""
    view2CreateTemplate = ""
    view3CreateTemplate = ""
    mergeSCD2Template = ""
    deleteTableDataTemplate = ""

    def __init__(self,cfg, meta, wStatus_suffix, outputErrorDF, logger):
        self.__cfg = cfg.copy()
        self.__meta = meta
        self.__logger = logger
        self.__outputErrorDF = outputErrorDF

        self.export_path = self.__cfg["informer"]["export_path" + wStatus_suffix]
        self.archive_path = self.__cfg["ccdw"]["archive_path" + wStatus_suffix]
        self.sql_schema_input =self.__cfg["sql"]["schema"]
        self.sql_schema_history =self.__cfg["sql"]["schema_history"]

        # Set template items
        self.tableNamesTemplate = self.LoadTemplate( "table_names" )
        self.tableColumnsNamesTemplate = self.LoadTemplate( "table_column_names" )
        self.dropViewTemplate = self.LoadTemplate( "drop_view" )
        self.viewCreateTemplate = self.LoadTemplate( "view_create" )
        self.alterTableKeyColumnTemplate = self.LoadTemplate( "alter_table_key_column" )
        self.alterTableKeysTemplate = self.LoadTemplate( "alter_table_keys" )
        self.alterTableTemplate = self.LoadTemplate( "alter_table_column" )
        self.view2CastTemplate = self.LoadTemplate( "view2_cast" )
        self.view2CrossApplyTemplate = self.LoadTemplate( "view2_crossapply" )
        self.view2WhereAndTemplate = self.LoadTemplate( "view2_whereand" )
        self.view2CreateTemplate = self.LoadTemplate( "view2_create" )
        self.view3CreateTemplate = self.LoadTemplate( "view3_create" )
        self.mergeSCD2Template = self.LoadTemplate( "merge_scd2" )
        self.deleteTableDataTemplate = self.LoadTemplate( "delete_table_data" )

        self.set_engine(self.__cfg["sql"]["driver"], self.__cfg["sql"]["server"], self.__cfg["sql"]["db"], self.__cfg["sql"]["schema"])

        self.LoadServerTableMetadata()

        self.__logger.debug("CCDW_Export initialized")

    def LoadServerTableMetadata(self, schema=""):

        if schema in [self.sql_schema_input,"input",""]:
            flds = { "TableSchema" : f"'{self.sql_schema_input}'" }
            sql_tbl_query = self.tableNamesTemplate.substitute(flds)
            self.svr_tables_input = pd.read_sql(sql_tbl_query, self.engine)
            self.svr_tables_input = np.asarray(self.svr_tables_input["TABLE_NAME"])

        if schema in [self.sql_schema_history,"history",""]:
            flds = { "TableSchema" : f"'{self.sql_schema_history}'" }
            sql_tbl_query = self.tableNamesTemplate.substitute(flds)
            self.svr_tables_history = pd.read_sql(sql_tbl_query, self.engine)
            self.svr_tables_history = np.asarray(self.svr_tables_history["TABLE_NAME"])

    def LoadServerColumnMetadata(self, sqlName, schema=""):
        if schema == "":
            schema = self.sql_schema_input

        flds = {"TableSchema" : f"'{schema}'", 
                "TableName"   : f"'{sqlName}'" }

        sql_col_query = self.tableColumnsNamesTemplate.substitute(flds)
        svr_columns= pd.read_sql(sql_col_query, self.engine)
        svr_columns.reset_index(inplace=True,drop=True)

        return(svr_columns)

    def LoadTemplate(self,template):
        filein = open(self.__cfg["sql"][template],'r')
        ReturnTemplate = Template( filein.read() )
        filein.close()
        return(ReturnTemplate)
    
    # engine() - creates an engine to be used to interact with the SQL Server
    @logger.catch
    def set_engine( self, driver, server, db, schema ):
        conn_details =  f"DRIVER={{{driver}}};SERVER={server};DATABASE={db};SCHEMA={schema};Trusted_Connection=Yes;"
        params = urllib.parse.quote_plus(conn_details)
        self.engine = sqlalchemy.create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
        return

    # executeSQL_UPDATE() - calls both executeSQL_INSERT and executeSQL_MERGE in attempt to update the SQL Tables 
    @logger.catch
    def executeSQL_UPDATE( self, sqlName, df ):
                        
        self.svr_columns_input = self.LoadServerColumnMetadata(sqlName,schema=self.sql_schema_input)
        self.svr_columns_history = self.LoadServerColumnMetadata(sqlName,schema=self.sql_schema_history)
        self.df_columns = df.columns

        self.keyList, \
        self.dataTypes, \
        self.sqlTypes, \
        self.dataTypeMV, \
        self.elementAssocTypes, \
        self.elementAssocNames = self.__meta.getDataTypes(columns=self.df_columns)

        try:
            self.__executeSQL_INSERT( sqlName, df ) 
        except:
            self.__logger.exception("XXXXXXX failed on executeSQL_INSERT XXXXXXX")
            raise

        try:
            self.__executeSQL_MERGE( sqlName, df ) 
        except:
            self.__logger.exception("XXXXXXX failed on executeSQL_MERGE XXXXXXX")
            raise

    # executeSQL_INSERT() - attempts to create SQL code from csv files and push it to the SQL server
    @logger.catch
    def __executeSQL_INSERT( self, sqlName, df ):

        self.__logger.debug("Fix all non-string columns, replace blanks with NAs which become NULLs in DB, and remove commas")
        nonstring_columns = [key for key in self.dataTypes.keys() & self.df_columns if type(self.dataTypes[key]) != sqlalchemy.sql.sqltypes.String]
        df[nonstring_columns] = df[nonstring_columns].replace({'':np.nan, ',':''}, regex=True) # 2018-06-18 C DMO

        # Attempt to push the new data to the existing SQL Table.
        try:
            new_columns = set(self.df_columns) - set(list(self.svr_columns_input["COLUMN_NAME"]))
            if len(self.svr_columns_input["COLUMN_NAME"]) != 0 and len(new_columns) != 0: 
                self.__logger.debug("Attempt to add new columns to table in SQL Server")
                self.__executeSQLAppend(df, sqlName, self.sql_schema_input)

        except:
            self.__logger.exception("Unknown error in executeSQLAppend: ",sys.exc_info()[0])
            raise

        try:
            self.__logger.debug( f"Push {sqlName} data to SQL schema {self.sql_schema_input}" )

            df.to_sql(sqlName, self.engine, schema=self.sql_schema_input, if_exists="append",
                    index=False, index_label=None, chunksize=None, dtype=self.dataTypes)

        except (exc.SQLAlchemyError, exc.DBAPIError, exc.ProgrammingError) as er:
            # This is a temporary line. Needs to be for ProgrammingError ONLY!
            self.__logger.debug( f"Error Msg: {str(er.orig.args[1])}" )
            self.__logger.exception( f"Error in File:\t{sqlName}\n\n Error: {er}\n DataTypes: {self.dataTypes}\n\n" )
            raise

        except:
            self.__logger.exception( f"Unknown error in executeSQL_INSERT: {sys.exc_info()[0]}" )
            raise    

    # executeSQLAppend() - Attempts to execute an Append statement to the SQL Database with new Columns
    @logger.catch
    def __executeSQLAppend(self, df, sqlName, schema):

        TableColumns= self.df_columns 

        self.__logger.debug("_______________________________________________________________________")
        if schema == self.sql_schema_input:
            newnames = set(TableColumns) - set(self.svr_columns_input["COLUMN_NAME"])
        elif schema == self.sql_schema_history:
            newnames = set(TableColumns) - set(self.svr_columns_history["COLUMN_NAME"])
        else:
            newnames = ""

        if not newnames:
            self.__logger.debug("No new columns")
            return()
        else:
            self.__logger.debug(f"Newnames = {newnames}")

        #attemp to create a dataframe and string of columns that need to be added to SQL Server
        try:
            updateFrame = pd.DataFrame(columns=newnames)
            updateColumns= list(updateFrame.columns)   # "_wStatus" if wStatus else ""
            updateColumns1 = ",\n\t".join(f"[{c}] {self.sqlTypes[c]}" for c in reversed(updateColumns))
        except:
            self.__logger.exception("ERROR!!!!")

        self.__logger.debug(f"UpdateColumns1: {updateColumns1}")
        
        #create SQL File based on tempalte to ALTER current SQL Table and append new Columns
        flds = {"TableSchema"          : schema, 
                "TableName"            : sqlName,
                "TableColumns"         : ", ".join(f"[{c}]" for c in TableColumns),
                "updateColumns"        : updateColumns1,
                "ViewName"             : f"{sqlName}_Current",
                "ViewSchema"           : self.sql_schema_history
            }

        alterTableSQL = self.alterTableTemplate.substitute(flds)
        view3CreateSQL = self.view3CreateTemplate.substitute(flds)
        dropViewSQL = self.dropViewTemplate.substitute(flds)

        self.__logger.debug("_______________________________________________________________________")
        
        #if there are added Columns attempt to push them to the SQL Table
        try:
            if (updateColumns):
                self.engine.execute(alterTableSQL)
                self.LoadServerTableMetadata(schema=schema)

        except (exc.SQLAlchemyError, exc.DBAPIError, exc.ProgrammingError) as er:
            self.__logger.error(f"-Error Updating Columns in SQL Table - [{str(er.args[0])}]")
            self.__logger.exception(f"Error in File: \t {sqlName}\n\n Error:{er}\n\n")
            ef = open(f"MergeError_{sqlName}.sql", 'w')
            ef.write(alterTableSQL)
            ef.close()
            raise

        if (updateColumns):
            try:
                self.__logger.debug("----Creating Current View")
                #drop sql view if exits
                self.engine.execute(dropViewSQL)
                #create new sql view
                self.engine.execute(view3CreateSQL)

            except (exc.SQLAlchemyError, exc.DBAPIError, exc.ProgrammingError) as er:
                self.__logger.error(f"-Error Creating View of SQL Table - [{str(er.args[0])}]" )
                self.__logger.exception(f"Error in Table: \t {sqlName}\n\n Error:{er}\n\n")
                raise

    # executeSQL_MERGE() - Creates SQL Code based on current Table/Dataframe by using a Template then pushes to History
    @logger.catch
    def __executeSQL_MERGE( self, sqlName, df ):
        
        # Get a list of all the keys for this table
        TableKeys = list(self.keyList.keys()) 

        TableDefaultDate = min(df["DataDatetime"])

        self.__logger.debug( "Create blank dataframe for output\n" )    

        # Create and push a blankFrame with no data to SQL Database for History Tables
        blankFrame = pd.DataFrame(columns=self.df_columns)
        # History does not use DataDatetime, so delete that
        del blankFrame["DataDatetime"]

        # Get a list of all the columns in this table. 
        # This is needed for the template below.
        TableColumns = list(blankFrame.columns) 
        
        # Add three History columns to blankFrame
        blankFrame["EffectiveDatetime"] = ""
        blankFrame["ExpirationDatetime"] = ""
        blankFrame["CurrentFlag"] = ""
        
        # Add three History column types to dataTypesDict
        blankTyper = self.dataTypes
        blankTyper["EffectiveDatetime"] = sqlalchemy.types.DateTime()
        blankTyper["ExpirationDatetime"] = sqlalchemy.types.DateTime()
        blankTyper["CurrentFlag"] = sqlalchemy.types.String(1)

        self.df_columns = blankFrame.columns

        flds_columns = {"TableSchema" : f"'{self.sql_schema_history}'", 
                        "TableName"   : f"'{sqlName}'" }

        if not sqlName in self.svr_tables_history:
            # This will create the history table 
            try:
                # Send the blank dataframe which creates missing tables
                blankFrame.to_sql(sqlName, self.engine, schema=self.sql_schema_history, if_exists="fail",
                                    index=False, index_label=None, chunksize=None, dtype=blankTyper)

            except (exc.SQLAlchemyError, exc.DBAPIError, exc.ProgrammingError) as er:
                self.__logger.exception( f"Error in File: \t{sqlName}\n\n Error: {er}\n\n" )
                raise

            except er:
                self.__logger.exception( f"Error: {er}" )
                raise

            else:

                # If it is the first time the history table is created then create a view as well
                try:
                    self.__logger.debug( "--Creating History Keys" )

                    # Set keys to not null
                    for key in self.keyList:

                        flds_keys = {"TableSchema"   : self.sql_schema_history,
                                     "TableName"     : sqlName,
                                     "TableKey"      : key,
                                     "TableKey_Type" : self.sqlTypes[key]
                                    }
                        alterTableKeyColumnSQL = self.alterTableKeyColumnTemplate.substitute(flds_keys)
                        self.engine.execute(alterTableKeyColumnSQL)

                    flds_keys = {"TableSchema"   : self.sql_schema_history,
                                 "TableName"     : sqlName,
                                 "TableKey"      : "EffectiveDatetime",
                                 "TableKey_Type" : self.sqlTypes["EffectiveDatetime"]
                                }
                    alterTableKeyColumnSQL = self.alterTableKeyColumnTemplate.substitute(flds_keys)
                    self.engine.execute(alterTableKeyColumnSQL)

                    flds_pk = {"TableSchema_DEST" : self.sql_schema_history,
                               "TableName"        : sqlName,
                               "pkName"           : f"pk_{sqlName}",
                               "primaryKeys"      : ", ".join(f"[{c}]" for c in self.keyList)
                              }
                    
                    alterTableKeysSQL = self.alterTableKeysTemplate.substitute(flds_pk)
                    self.engine.execute(alterTableKeysSQL)

                except:
                    self.__logger.debug("-Error Creating History Keys for SQL Table" )
                    self.__logger.debug(alterTableKeyColumnSQL)
                    self.__logger.debug(alterTableKeysSQL)
                    raise

                self.svr_columns_history = self.LoadServerColumnMetadata(sqlName,schema=self.sql_schema_history)
                self.LoadServerTableMetadata(schema=self.sql_schema_history)

            flds = {"TableSchema_SRC"      : self.sql_schema_input, 
                    "TableSchema_DEST"     : self.sql_schema_history,
                    "TableName"            : sqlName,
                    "TableKeys"            : ", ".join(f"[{k}]".format(k) for k in TableKeys),
                    "TableKeys_wTypes"     : ", ".join(f"{k} [{self.sqlTypes[k]}]" for k in TableKeys),
                    "TableColumns"         : ", ".join(f"[{c}]" for c in TableColumns),
                    "TableKeys_CMP"        : " AND ".join(f"DEST.[{k}] = SRC.[{k}]" for k in TableKeys),
                    "TableKeys_SRC"        : ", ".join(f"SRC.[{k}]" for k in TableKeys),
                    "TableColumns_SRC"     : ", ".join(f"SRC.[{c}]" for c in TableColumns),
                    "TableColumns1_SRC"    : ", ".join([]), # There are no Type 1 SCD at this time
                    "TableColumns1_DEST"   : ", ".join([]), # There are no Type 1 SCD at this time
                    "TableColumns1_UPDATE" : ", ".join([]), # There are no Type 1 SCD at this time
                    "TableColumns2_SRC"    : ", ".join(f"SRC.[{c}]" for c in TableColumns),
                    "TableColumns2_DEST"   : ", ".join(f"DEST.[{c}]" for c in TableColumns),
                    "TableDefaultDate"     : TableDefaultDate,
                    "ViewSchema"           : self.sql_schema_history,
                    "ViewName"             : f"{sqlName}_Current",
                    "ViewName2"            : f"{sqlName}_test",
                    "pkName"               : f"pk_{sqlName}",
                    "primaryKeys"          : ", ".join(f"[{k}]" for k in self.keyList),
                    "ViewColumns"          : ", ".join(f"[{c}]" for c in TableColumns)
            }

            try:
                dropViewSQL = self.dropViewTemplate.substitute(flds)
                createViewSQL = self.viewCreateTemplate.substitute(flds)

                self.__logger.debug(f"--Creating History View {self.sql_schema_history}.{flds['ViewName']} (dropping if exists)")
                #drop sql view if exits
                self.engine.execute(dropViewSQL)

                self.__logger.debug(f"View {self.sql_schema_history}.{flds['ViewName']} DDL:")
                self.__logger.debug(createViewSQL)

                #create new sql view
                self.engine.execute(createViewSQL)

            except:
                self.__logger.exception ("-Error Creating View of SQL Table" )
                self.__logger.debug(dropViewSQL)
                raise

            try:
                elementAssocNamesSet = set( val for dic in [self.elementAssocNames] for val in dic.values())
                for elementAssocName in elementAssocNamesSet: 
                    self.__logger.debug(f"Processing association {elementAssocName}")
                    associationKey = ''
                    lastelement = ''
                    view2CastSQL = ''
                    view2CrossApplySQL = ''
                    view2WhereAndSQL = ''
                    counter = 0

                    for elementAssocIndex, elementAssocKey in enumerate(list(self.elementAssocNames)):
                        if (self.elementAssocNames[elementAssocKey] == elementAssocName):
                            self.__logger.debug(f"Found element of association: {elementAssocKey}")
                            counter += 1

                            flds_view2 = {"ItemType"              : self.dataTypeMV[elementAssocKey],
                                          "Counter"               : counter,
                                          "ElementAssociationKey" : elementAssocKey
                                         }
                            # associationGroup = elementAssocNamesDict[]

                            view2CastSQL += self.view2CastTemplate.substitute(flds_view2)
                            view2CrossApplySQL += self.view2CrossApplyTemplate.substitute(flds_view2)

                            if(counter > 1):
                                self.__logger.debug(f"Counter > 1, adding comparison of 1 and {counter}")
                                view2WhereAndSQL += self.view2WhereAndTemplate.substitute(flds_view2)

                            # If single MV is not a key, need to force it to be one
                            lastelement = elementAssocKey

                            if (self.elementAssocTypes[elementAssocKey] == 'K'):
                                associationKey = elementAssocKey
                                view_Name = self.elementAssocNames[elementAssocKey]
                                self.__logger.debug(f"Add key {elementAssocKey} to association {view_Name.replace('.', '_')}")

                    if (associationKey == ''):
                        if (lastelement != ''):
                            associationKey = lastelement
                            view_Name = self.elementAssocNames[associationKey]
                            self.__logger.debug(f"Setting associationKey to {lastelement} for association {view_Name.replace('.', '_')}")

                        else:
                            self.__logger.debug(f"No associationKey to set for association {elementAssocName}")
                            raise(AssertionError)

                    view2_Str = f"{sqlName}__{view_Name.replace('.', '_')}"

                    flds2 = {
                            "TableName"       : sqlName,
                            "ViewSchema"      : self.sql_schema_history,
                            "ViewName"        : view2_Str,
                            "primaryKeys"     : ", ".join(f"[{c}]" for c in self.keyList),
                            "CastStr"         : view2CastSQL,
                            "CrossApplyStr"   : view2CrossApplySQL,
                            "WhereAndStr"     : view2WhereAndSQL,
                            "associationKeys" : associationKey
                    }

                    dropViewSQL = self.dropViewTemplate.substitute(flds2)
                    createView2SQL = self.view2CreateTemplate.substitute(flds2)

                    self.__logger.debug(f"View {view2_Str} DDL:")
                    self.__logger.debug(createView2SQL)

                    self.__logger.debug(f"--Creating History View2 {self.sql_schema_history}.{view2_Str} (dropping if exists)")
                    #drop sql view if exits
                    self.engine.execute(dropViewSQL)
                    self.engine.execute(createView2SQL)

            except:
                self.__logger.exception("Creating View2 failed")
                ef = open(f"View2Error_{view2_Str}.sql", 'w')
                ef.write(createView2SQL)
                ef.close()
                raise

        else:
            pass

         #Attempt to push the Column data to the existing SQL Table if there are new Columns to be added.
        try:
            self.__executeSQLAppend(blankFrame, sqlName, self.sql_schema_history)
            
        except:
            self.__logger.exception("append didnt work")
            raise

        flds = {"TableSchema_SRC"      : self.sql_schema_input, 
                "TableSchema_DEST"     : self.sql_schema_history,
                "TableName"            : sqlName,
                "TableKeys"            : ", ".join(f"[{k}]" for k in TableKeys),
                "TableKeys_wTypes"     : ", ".join(f"{k} [{self.sqlTypes[k]}]" for k in TableKeys),
                "TableColumns"         : ", ".join(f"[{c}]" for c in TableColumns),
                "TableKeys_CMP"        : " AND ".join(f"DEST.[{k}] = SRC.[{k}]" for k in TableKeys),
                "TableKeys_SRC"        : ", ".join(f"SRC.[{k}]" for k in TableKeys),
                "TableColumns_SRC"     : ", ".join(f"SRC.[{c}]" for c in TableColumns),
                "TableColumns1_SRC"    : ", ".join([]), # There are no Type 1 SCD at this time
                "TableColumns1_DEST"   : ", ".join([]), # There are no Type 1 SCD at this time
                "TableColumns1_UPDATE" : ", ".join([]), # There are no Type 1 SCD at this time
                "TableColumns2_SRC"    : ", ".join(f"SRC.[{c}]" for c in TableColumns),
                "TableColumns2_DEST"   : ", ".join(f"DEST.[{c}]" for c in TableColumns),
                "TableDefaultDate"     : TableDefaultDate,
                "viewSchema"           : self.sql_schema_history,
                "viewName"             : f"{sqlName}_Current",
                "ViewName2"            : f"{sqlName}_test",
                "pkName"               : f"pk_{sqlName}",
                "primaryKeys"          : ", ".join(f"[{k}]" for k in self.keyList),
                "viewColumns"          : ", ".join(f"[{c}]" for c in TableColumns)
        }

        mergeSCD2SQL = self.mergeSCD2Template.substitute(flds)

        # Attempt to execute generated SQL MERGE code
        try:
            self.__logger.debug("...executing sql command")
            rtn = self.engine.execute(mergeSCD2SQL)

        except (exc.SQLAlchemyError, exc.DBAPIError, exc.ProgrammingError, pyodbc.Error, pyodbc.ProgrammingError) as er:
            self.__logger.error(f"---executing sql command - skipped SQL ERROR [{str(er.args[0])}]")
            self.__logger.exception(f"Error in File: \t {sqlName}\n\n Error: {er}\n\n\n")
            ef = open(f"MergeError_{sqlName}.sql", 'w')
            ef.write(mergeSCD2SQL)
            ef.close()
            raise

        # Deletes Tables from SQL Database if coppied to the history table
        try:
            self.__logger.debug("...executing delete command")

            flds_del = {"TableSchema" : self.sql_schema_input, 
                        "TableName"   : sqlName,
                    }
            deleteDataSQL = self.deleteTableDataTemplate.substitute(flds_del)

            rtn = self.engine.execute(deleteDataSQL)

        except (exc.SQLAlchemyError, exc.DBAPIError, exc.ProgrammingError, pyodbc.Error, pyodbc.ProgrammingError) as er:
            self.__logger.error(f"---executing DELETE command - skipped SQL ERROR [{str(er.args[0])}]")
            self.__logger.exception(f"Error in File: \t {sqlName}\n\n Error: {er}\n\n\n")
            raise

        self.__logger.debug("....wrote to history")
