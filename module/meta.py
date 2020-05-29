import sys
import os
from os import path
import pandas as pd
import glob
from loguru import logger
import sqlalchemy
from sqlalchemy.dialects import mssql
from typing import TypeVar, List, Tuple, Dict

MetaObject = TypeVar('MetaObject')

class CCDW_Meta:
    __lookuplist = None
    __cfg = {}
    __logger = None

    def __init__( self, cfg: dict ):
        self.__cfg = cfg.copy()
        self.__logger = self.__cfg['__local']['logger']

        self.loadLookupList( cfg )

        self.__logger.debug("CCDW_Meta initialized")

    @logger.catch
    def loadLookupList(self, engine: object = None, refresh: bool = False) -> None:

        if not self.__lookuplist or refresh:
            # Read all files in the meta folder
            meta_path = self.__cfg["informer"]["export_path_meta"]
            all_files = glob.glob(os.path.join(meta_path, "*_CDD*csv"))
            df_from_each_file = (pd.read_csv(f,encoding = "ansi", dtype="str", index_col=0) for f in all_files)
            self.__lookuplist = pd.concat(df_from_each_file)
            
            meta_custom = self.__cfg["ccdw"]["meta_custom"]
            meta_custom_csv = pd.read_csv(meta_custom,encoding = "ansi", dtype="str", index_col=0)

            self.__lookuplist = self.__lookuplist.append(meta_custom_csv)
            self.__lookuplist.index.rename("DATA.ELEMENT",inplace=True)
            self.__lookuplist.reset_index(inplace=True)
            self.__lookuplist = self.__lookuplist.drop_duplicates(subset="DATA.ELEMENT", keep="last")
            self.__lookuplist = self.__lookuplist.where(pd.notnull(self.__lookuplist), None)
            self.__lookuplist.set_index("DATA.ELEMENT",inplace=True)
        
        return

    # Return the key(s) for the specified fle
    @logger.catch
    def getKeyFields(self, file:str = "") -> List[str]:
        if file=='':
            keys = self.__lookuplist[(self.__lookuplist["DATABASE.USAGE.TYPE"]=='K')]
        else:
            keys = self.__lookuplist[(self.__lookuplist["SOURCE"]==file) & (self.__lookuplist["DATABASE.USAGE.TYPE"]=='K')]

        keys.reset_index(inplace=True)
        return(list(keys["DATA.ELEMENT"]))

    @logger.catch
    def getDataTypes(self, file: str = "", columns: List[str] = []) -> Tuple[Dict[str,str],Dict[str,str],Dict[str,str],Dict[str,str],Dict[str,str],Dict[str,str]]:

        src_file = file.replace('_','.')

        if src_file!='':
            dtLookuplist = self._CCDW_Meta__lookuplist.loc[self._CCDW_Meta__lookuplist["SOURCE"].isin([src_file,"SYS_CCDW"])]
        else:
            if len(columns) > 0:
                dtLookuplist_ccdw = self._CCDW_Meta__lookuplist.loc[self._CCDW_Meta__lookuplist["SOURCE"]=="SYS_CCDW"]
                all_columns = columns.copy()
                all_columns.extend(list(dtLookuplist_ccdw.index))
                all_columns = sorted([*{*all_columns}])

                dtLookuplist = self._CCDW_Meta__lookuplist.loc[all_columns]
            else:
                dtLookuplist = self._CCDW_Meta__lookuplist

        fieldNames = dtLookuplist.index.array
        dataType = dtLookuplist["DATA.TYPE"].copy()
        sqlType = dtLookuplist["DATA.TYPE"].copy()
        dataTypeMV = dtLookuplist["DATA.TYPE"].copy()
        dataLength = dtLookuplist["DEFAULT.DISPLAY.SIZE"].copy()
        usageType = dtLookuplist["DATABASE.USAGE.TYPE"].copy()
        elementAssocType = dtLookuplist["ELEMENT.ASSOC.TYPE"].copy()
        elementAssocName = dtLookuplist["ELEMENT.ASSOC.NAME"].copy()
        dataDecimalLength = dtLookuplist["DT2"].replace('', '0', regex=True).copy()

        if len(columns) == 0:
            columns = fieldNames.copy()
        else:
            if file != "":
                columns = columns.append(dtLookuplist_ccdw.index)

        for index, fieldDataType in dataType.iteritems():
            dtypers = "VARCHAR(MAX)"
            sqlType[index] = "VARCHAR(MAX)"

            if usageType[index] in ['A','Q','L','I','C']:
                if fieldDataType in ['S','U','',None]:
                    dtypers = f"VARCHAR({dataLength[index]})"
                elif fieldDataType == 'T':
                    dtypers = "TIME"
                elif fieldDataType == 'N':
                    dtypers = f"NUMERIC({dataLength[index]}, {dataDecimalLength[index]})"
                elif fieldDataType == 'D':
                    dtypers = "DATE"
                elif fieldDataType == "DT":
                    dtypers = "DATETIME"
                dataTypeMV[index] = dtypers
                dataType[index] = sqlalchemy.types.String(None)
                sqlType[index] = f"VARCHAR(MAX)"

            elif fieldDataType in ['S','U','',None]:
                dataType[index] = mssql.VARCHAR(dataLength[index])
                sqlType[index] = f"VARCHAR({dataLength[index]})"

            elif fieldDataType == 'T':
                dataType[index] = mssql.TIME
                sqlType[index] = "TIME"

            elif fieldDataType == 'N':
                if dataDecimalLength[index] and dataDecimalLength[index] != '0':
                    dataType[index] = mssql.NUMERIC(int(dataLength[index]),int(dataDecimalLength[index]))
                    sqlType[index] = f"NUMERIC({dataLength[index]},{dataDecimalLength[index]})"
                else:
                    if int(dataLength[index]) <= 2:
                        dataType[index] = mssql.TINYINT
                        sqlType[index] = "TINYINT"
                    elif int(dataLength[index]) <= 4:
                        dataType[index] = mssql.SMALLINT
                        sqlType[index] = "SMALLINT"
                    elif int(dataLength[index]) <= 9:
                        dataType[index] = mssql.INTEGER
                        sqlType[index] = "INTEGER"
                    else:
                        dataType[index] = mssql.BIGINT
                        sqlType[index] = "BIGINT"

            elif fieldDataType == 'D':
                dataType[index] = mssql.DATE
                sqlType[index] = "DATE"

            elif fieldDataType == "DT":
                dataType[index] = mssql.DATETIME
                sqlType[index] = "DATETIME"

            else:
                dataType[index] = mssql.VARCHAR(None)
                sqlType[index] = "VARCHAR(MAX)"

        keyList = dict(list(zip(fieldNames,usageType)))
        dataTypes = dict(list(zip(fieldNames,dataType)))
        sqlTypes = dict(list(zip(fieldNames,sqlType)))
        dataTypeMV = dict(list(zip(fieldNames,dataTypeMV)))
        elementAssocTypes = dict(list(zip(fieldNames,elementAssocType)))
        elementAssocNames = dict(list(zip(fieldNames,elementAssocName)))

        # Remove blank entries from the association and multi-value dictionaries (not every field is multi-valued or in an association)
        for key, val in list(elementAssocNames.items()):
            if val == None:
                del elementAssocNames[key]
                del elementAssocTypes[key]
                del dataTypeMV[key]

        for key, val in list(keyList.items()):
            if val != 'K':
                del keyList[key]

        return(keyList,dataTypes,sqlTypes,dataTypeMV,elementAssocTypes,elementAssocNames)
