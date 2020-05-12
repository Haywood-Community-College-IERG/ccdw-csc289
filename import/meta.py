import sys
import os
from os import path
#import csv
#import yaml
#import urllib
import pandas as pd
#import numpy as np
import glob
#import export
from loguru import logger
import sqlalchemy

#import functools

class CCDW_Meta:
    __lookuplist = None
    __cfg = []
    __logger = None

    def __init__( self, cfg, logger ):
        self.__cfg = cfg.copy()
        self.__logger = logger

        self.loadLookupList( cfg )

        self.__logger.debug("CCDW_Meta initialized")

    @logger.catch
    def loadLookupList(self, engine=None, refresh=False):

        if not self.__lookuplist or refresh:
            # Read all files in the meta folder
            meta_path = self.__cfg["informer"]["export_path_meta"]
            all_files = glob.glob(os.path.join(meta_path, "*_CDD*csv"))
            df_from_each_file = (pd.read_csv(f,encoding = "ansi", dtype="str", index_col=0) for f in all_files)
            self.__lookuplist = pd.concat(df_from_each_file)
            
            meta_custom = self.__cfg["ccdw"]["meta_custom"]
            meta_custom_csv = pd.read_csv(meta_custom,encoding = "ansi", dtype="str", index_col=0)

            self.__lookuplist = self.__lookuplist.append(meta_custom_csv)
            self.__lookuplist.reset_index(inplace=True)
            self.__lookuplist = self.__lookuplist.drop_duplicates(subset="ids", keep="last")
            self.__lookuplist = self.__lookuplist.where(pd.notnull(self.__lookuplist), None)
            self.__lookuplist.set_index("ids",inplace=True)
        
        return

    # Return the key(s) for the specified fle
    @logger.catch
    def getKeyFields(self, file=''):
        if file=='':
            keys = self.__lookuplist[(self.__lookuplist["Database Usage Type "]=='K')]
        else:
            keys = self.__lookuplist[(self.__lookuplist["Source "]==file) & (self.__lookuplist["Database Usage Type "]=='K')]

        return(list(set(keys.ids.tolist())))

    @logger.catch
    def getDataTypes(self, file='', columns=[]):

        src_file = file.replace('_','.')

        if file!='':
            dtLookuplist_file = self._CCDW_Meta__lookuplist.loc[self._CCDW_Meta__lookuplist["Source "]==src_file]
            dtLookuplist_ccdw = self._CCDW_Meta__lookuplist.loc[self._CCDW_Meta__lookuplist["Source "]=="SYS_CCDW"]
            dtLookuplist = dtLookuplist_file.append( dtLookuplist_ccdw )
        else:
            if not columns.empty:
                dtLookuplist_file = self._CCDW_Meta__lookuplist.loc[columns]
                dtLookuplist_ccdw = self._CCDW_Meta__lookuplist.loc[self._CCDW_Meta__lookuplist["Source "]=="SYS_CCDW"]
                dtLookuplist = dtLookuplist_file.append( dtLookuplist_ccdw )
            else:
                dtLookuplist = self._CCDW_Meta__lookuplist

        dtLookuplist.index.rename("ids",inplace=True)
        dtLookuplist.reset_index(inplace=True)
        dtLookuplist.drop_duplicates(subset=["ids"],inplace=True)
        dtLookuplist.set_index("ids",inplace=True)

        fieldNames = dtLookuplist.index.array
        dataType = dtLookuplist["Data Type "].copy()
        sqlType = dtLookuplist["Data Type "].copy()
        dataTypeMV = dtLookuplist["Data Type "].copy()
        dataLength = dtLookuplist["Default Display Size "].copy()
        usageType = dtLookuplist["Database Usage Type "].copy()
        elementAssocType = dtLookuplist["Element Assoc Type "].copy()
        elementAssocName = dtLookuplist["Element Assoc Name "].copy()
        dataDecimalLength = dtLookuplist["Dt2 "].replace('', '0', regex=True).copy()

        if columns.empty:
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
                dataType[index] = sqlalchemy.types.String(dataLength[index])
                sqlType[index] = f"VARCHAR({dataLength[index]})"

            elif fieldDataType == 'T':
                dataType[index] = sqlalchemy.types.Time()
                sqlType[index] = "TIME"

            elif fieldDataType == 'N':
                dataType[index] = sqlalchemy.types.Numeric(int(dataLength[index]),dataDecimalLength[index])
                sqlType[index] = f"NUMERIC({int(dataLength[index])},{dataDecimalLength[index]})"

            elif fieldDataType == 'D':
                dataType[index] = sqlalchemy.types.Date()
                sqlType[index] = "DATE"

            elif fieldDataType == "DT":
                dataType[index] = sqlalchemy.types.DateTime()
                sqlType[index] = "DATETIME"

            else:
                dataType[index] = sqlalchemy.types.String(None)
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