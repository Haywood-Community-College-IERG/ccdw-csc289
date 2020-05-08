import csv
import os
import sys
#import yaml
#import urllib
import pandas as pd
import numpy as np
from os import path
import glob
import export
from loguru import logger
import sqlalchemy

import functools

@logger.catch
def loadLookupList(cfg, engine, refresh=False):

    # Read all files in the meta folder
    meta_path = cfg['informer']['export_path_meta']
    all_files = glob.glob(os.path.join(meta_path, "*_CDD*csv"))
    df_from_each_file = (pd.read_csv(f,encoding = "ansi", dtype='str') for f in all_files)
    lookuplist = pd.concat(df_from_each_file, ignore_index=True)
    
    meta_custom = cfg['ccdw']['meta_custom']
    meta_custom_csv = pd.read_csv(meta_custom,encoding = "ansi", dtype='str')

    metaList = set(meta_custom_csv['ids'].copy())
    lookeyList = lookuplist['ids'].copy()
    meta_custom_csv = meta_custom_csv.sort_values(by='ids')
    delThis = [item for i, item in enumerate(lookeyList) if item in metaList]
    delIDs = [list(lookeyList).index(item) for i,item in enumerate(delThis)]
    meta_custom_csv.set_index('ids')
    lookuplist.drop(delIDs,axis=0, inplace=True)
    lookuplist = lookuplist.append(meta_custom_csv, ignore_index=True)
    lookuplist = lookuplist.where(pd.notnull(lookuplist), None)
    lookuplist.set_index('ids')

    if refresh:
        logger.debug( "Update CDD in meta" )
        logger.debug( "...delete old data" )
        engine.execute( 'DELETE FROM meta.CDD' )
        logger.debug( "...push new data" )
        lookuplist.to_sql( 'CDD', engine, flavor=None, schema='meta', if_exists='append',
                            index=False, index_label=None, chunksize=None )
        logger.debug( "...Update CDD in meta [DONE]" )
    
    return(lookuplist)

#loadLookupList()

# Return the key(s) for the specified fle
@logger.catch
def getKeyFields(cfg, engine, lookuplist, file=''):
    if file=='':
        keys = lookuplist[(lookuplist['Database Usage Type ']=='K')]
    else:
        keys = lookuplist[(lookuplist['Source ']==file) & (lookuplist['Database Usage Type ']=='K')]

    return(list(set(keys.ids.tolist())))

@logger.catch
def getDataTypes(cfg, engine, lookuplist, file=''):

    if file!='':
        dtLookuplist = lookuplist[(lookuplist['Source ']==file)]
    else:
        dtLookuplist = lookuplist

    fieldNames = dtLookuplist['ids'].copy()
    dataType = dtLookuplist['Data Type '].copy()
    dataTypeMV = dtLookuplist['Data Type '].copy()
    dataLength = dtLookuplist['Default Display Size ']
    usageType = dtLookuplist['Database Usage Type '].copy()
    elementAssocType = dtLookuplist['Element Assoc Type '].copy()
    elementAssocName = dtLookuplist['Element Assoc Name '].copy()
    dataDecimalLength = dtLookuplist['Dt2 '].replace('', '0', regex=True).copy()

    for index, fieldDataType in enumerate(dataType):
        dtypers = "VARCHAR(MAX)"
        if usageType[index] == 'A' or usageType[index] == 'Q' or usageType[index] == 'L' or usageType[index] == 'I' or usageType[index] == 'C':
            if fieldDataType == 'S' or fieldDataType == 'U' or fieldDataType == '' or fieldDataType == None:
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
            fieldDataType = sqlalchemy.types.String(None) # changed from 8000

        elif fieldDataType == 'S' or fieldDataType == 'U' or fieldDataType == '' or fieldDataType == None:
            fieldDataType = sqlalchemy.types.String(dataLength[index]) #types = sqlalchemy.types.String(8000)

        elif fieldDataType == 'T':
            fieldDataType = sqlalchemy.types.Time() # 'TIME'

        elif fieldDataType == 'N':
            fieldDataType = sqlalchemy.types.Numeric(int(dataLength[index]),dataDecimalLength[index]) #'DECIMAL(' + str(dataLength[index]) + ',3)'

        elif fieldDataType == 'D':
            fieldDataType = sqlalchemy.types.Date()  # 'DATE'

        elif fieldDataType == 'DT':
            fieldDataType = sqlalchemy.types.DateTime()

        dataType[index] = fieldDataType

    keyListDF = pd.concat([fieldNames,usageType], axis=1)
    dataTypeMV = pd.concat([fieldNames,dataTypeMV], axis=1)
    dataTypes = pd.concat([fieldNames,dataType], axis=1)
    elementAssocTypes = pd.concat([fieldNames,elementAssocType], axis=1)
    elementAssocNames = pd.concat([fieldNames,elementAssocName], axis=1)


    keyList = list(keyListDF.set_index('ids').to_dict().values()).pop()
    dataTypes = list(dataTypes.set_index('ids').to_dict().values()).pop()
    dataTypeMV = list(dataTypeMV.set_index('ids').to_dict().values()).pop()
    elementAssocTypes = list(elementAssocTypes.set_index('ids').to_dict().values()).pop()
    elementAssocNames = list(elementAssocNames.set_index('ids').to_dict().values()).pop()

    return(keyList,dataTypes,dataTypeMV,elementAssocTypes,elementAssocNames)