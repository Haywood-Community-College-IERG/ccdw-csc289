import os
import glob
import sys
import yaml
import pandas as pd
import pathlib
import numpy as np
import datetime
import argparse
import regex
from datetime import date, timedelta
from loguru import logger

import functools

global writedb
global diffs
global refresh
global cfg

run_datetime = datetime.datetime.now().strftime("%Y-%m-%d_%H%M%S%f")
def timestamp():
    return( datetime.datetime.now().isoformat() )

# This will import the config.py module in the current directory
import config
config.load_cfg()
cfg = config.cfg 

parser = argparse.ArgumentParser(description='Import CCDW data')
parser.add_argument('--nodb', dest='writedb', action='store_false', default=True,
                    help='Do not write to database (default: write to database)')
parser.add_argument('--diffs', dest='diffs', action='store_true', default=False,
                    help='Files to import are diffs already (default: using original files)')
parser.add_argument('--refresh', dest='refresh', action='store_true', default=False,
                    help='Refresh table and view structures (default: don''t refresh)')
parser.add_argument('--wStatus', dest='wStatus', action='store_true', default=False,
                    help='Refresh table and view structures (default: don''t refresh)')
parser.add_argument('--updateConfig', dest='updateConfig', action='store_true', default=False,
                    help='Refresh configuration in database (default: don''t refresh)')
args = parser.parse_args()

writedb = args.writedb
diffs = args.diffs
refresh = args.refresh
wStatus = args.wStatus
updateConfig = args.updateConfig

wStatus_suffix = "_wStatus" if wStatus else ""

# These are some common variables the program needs throughout
export_path = cfg['informer']['export_path' + wStatus_suffix]
archive_path = cfg['ccdw']['archive_path' + wStatus_suffix]
log_path = cfg['ccdw']['log_path']

prefix = cfg['informer']['prefix']

# We are transitioning the code from one type of logging to another. These opens both.
logger_log = open(os.path.join(log_path,"logger.log_{0}{1}.txt".format( run_datetime, wStatus_suffix )), "w", 1)

# Setup the new logger
logger.remove()
# logger.add( "logger.log_{time}{wStatus_suffix}.txt", wStatus_suffix )
logger.add(logger_log, enqueue=True, backtrace=True, diagnose=True) # Set last 2 to False
logger.debug( "Arguments: writedb = [{0}], diffs = [{1}], refresh = [{2}], wStatus = [{3}], updateConfig=[{4}]".format( writedb, diffs, refresh, wStatus, updateConfig ) )
    
# Import other local packages
import meta
import export

# This creates the SQL engine for pushing data to SQL Server
engine = export.engine(cfg['sql']['driver'], cfg['sql']['server'], cfg['sql']['db'], cfg['sql']['schema'])

# Get the keys, data types, and associations from the metadata (see meta.py)
keyList, dataTypes, dataTypeMV, elementAssocTypes, elementAssocNames = meta.getDataTypes()

# Push the 'school' section of the configuration to SQL Server, if requested
if updateConfig:
    logger.info("Update configuration")
    school = cfg['school']
    schooldf = pd.DataFrame(school, index = ['config'])
    schooldf.to_sql('config', engine, flavor=None, schema='local', if_exists='replace',
                 index=False, index_label=None, chunksize=None)

# Some files from Colleague have statuses with status dates. When building the database,
# we will use special Informer reports to load the data. This block of code is used for that.
#
# CSC-289 Students: You can ignore this section as the database already exists and has been loaded. This block will never be executed during class.
#
if wStatus:
    invalid_path = cfg['ccdw']['invalid_path_wStatus']
    
    pattern = r'{0}(?P<fnpat>.*)___.*|(?P<fnpat>.*)___.*'.format(prefix)

    # Some files have dates that are way out there. Let's mark as invalid those that are more than a year out
    invalid_date = date.today() + timedelta(365)

    # All the status fields in association with one another
    status_fields = { 
            'ACAD_PROGRAMS'     : ['ACPG.STATUS', 'ACPG.STATUS.DATE'],
            'APPLICATIONS'      : ['APPL.STATUS', 'APPL.STATUS.DATE', 'APPL.STATUS.TIME'],
            'COURSES'           : ['CRS.STATUS',  'CRS.STATUS.DATE'],
            'STUDENT_ACAD_CRED' : ['STC.STATUS',  'STC.STATUS.DATE',  'STC.STATUS.TIME', 'STC.STATUS.REASON'],
            'STUDENT_PROGRAMS'  : ['STPR.STATUS', 'STPR.STATUS.DATE', 'STPR.STATUS.CHGOPR'],
            'STUDENT_TERMS'     : ['STTR.STATUS', 'STTR.STATUS.DATE']
        }

    # Extract just the date and time fields
    status_datetime_fields = {}
    date_regex = regex.compile('.*\.DATE$|.*\.TIME$')
    for key in status_fields.keys():
        fields = status_fields[key]
        status_datetime_fields[key] = [ f for f in fields if date_regex.match(f) ]

    # This function is a helper function for processing all the rows in the file for a specified date
    def processfile(df, fn, d):
        logger.debug("Updating fn = "+fn+", d = "+d)
        columnHeaders = list(df.columns.values)
        columnArray = np.asarray(columnHeaders)

        # The following makes a copy of each for the columns in this data frame
        keyListDict = {k: keyList[k] for k in keyList.keys() & columnArray}   # a dictionary of Columns and their keys
        dataTypesDict = {k: dataTypes[k] for k in dataTypes.keys() & columnArray}   # a dictionary of Columns and their types
        dataTypeMVDict = {k: dataTypeMV[k] for k in dataTypeMV.keys() & columnArray}   # a dictionary of Columns and their multi-value types
        elementAssocTypesDict = {k: elementAssocTypes[k] for k in elementAssocTypes.keys() & columnArray}   # a dictionary of Columns and their Element Association Types
        elementAssocNamesDict = {k: elementAssocNames[k] for k in elementAssocNames.keys() & columnArray}   # a dictionary of Columns and their Element Association Names

        # Remove blank entries from the association and multi-value dictionaries (not every field is multi-valued or in an association)
        for key, val in list(elementAssocNamesDict.items()):
            if val == None:
                del elementAssocNamesDict[key]
                del elementAssocTypesDict[key]
                del dataTypeMVDict[key]

        for key, val in list(keyListDict.items()):
            if val != 'K':
                del keyListDict[key]

        try:
            export.executeSQL_UPDATE(engine, df, fn, keyListDict, dataTypesDict, dataTypeMVDict, elementAssocTypesDict, elementAssocNamesDict, logger)

            # Define and create the directory for all the output files
            # directory = '{path}/{folder}'.format(path=invalid_path,folder=fn)
            # os.makedirs(directory,exist_ok=True)
            # df.to_csv('{path}/{fn}_{dt}.csv'.format(path=directory,fn=fn,dt=d), index = False)

        except:
            logger.error('---Error in file: %s the folder will be skipped' % file)
            raise

        return

    # Cycle through all the files in the export folder
    for root, subdirs, files in os.walk(export_path):
        for file in files:
            with open(root + '/' + file, "r") as csvinput:
                # We only want to process files that match the pattern for wStatus files
                m = regex.match(pattern,file)
                if m==None:
                    continue
                fn = m.expandf("{fnpat}")

                # Get status and datetime fields for this file
                df_status = status_fields[fn]
                df_status_datetime = status_datetime_fields[fn]
                df_status_only = set(df_status).symmetric_difference(set(df_status_datetime))

                logger.debug("Processing "+fn+"...")

                # Read the file in and get the keys for this file
                df = pd.read_csv(csvinput,encoding='ansi',dtype='str',engine='python')
                file_keys = meta.getKeyFields(fn.replace('_','.'))

                # Fill down the status fields so all status fields have a value            
                for fld in df_status:
                    df[fld] = df.groupby(file_keys)[fld].ffill()

                # If the date field is still blank (i.e., was never provided), set it
                df[df_status_datetime[0]].fillna('1900-01-01', inplace=True)

                # Create a new DataDatetime field using the date and time fields.
                # If the time field is still blank, set it. If it is missing, set it.
                if len(df_status_datetime)==2:
                    df[df_status_datetime[1]].fillna('00:00:00', inplace=True)
                    newDataDatetime = df[df_status_datetime[0]] + "T" + df[df_status_datetime[1]]
                else:
                    newDataDatetime = df[df_status_datetime[0]] + "T00:00:00"

                # Make datetime variable an actual datetime instead of a string
                df['DataDatetime']=pd.to_datetime(newDataDatetime)

                # Keep the latest record of any duplicated rows by all columns except the status fields
                df = df.drop_duplicates(set(df.columns).symmetric_difference(set(df_status_only)),keep='last')

                # Sort by the DataDatetime
                df.sort_values(by='DataDatetime')

                # Define and create the directory for the INVALID output file
                os.makedirs(invalid_path,exist_ok=True)

                # Remove from the dataframe all rows with an invalid date
                # Keep the status date/time fields as string, as that is what they are in the database
                df[pd.to_datetime(df[df_status_datetime[0]])>invalid_date].to_csv('{path}/{fn}_INVALID.csv'.format(path=invalid_path,fn=fn))
                df = df[pd.to_datetime(df[df_status_datetime[0]])<=invalid_date]

                try:
                    # Now, group by the date field and create cumulative files for each date in the file
                    for d in sorted(df['DataDatetime'].dt.strftime('%Y-%m-%d').unique()):
                        processfile(df.loc[df[df_status_datetime[0]] == d].groupby(file_keys,as_index=False).last(), fn, d)

                    # If you want cumulative files (i.e., all the most recent records up to this date), use this
                    #for d in sorted(df['DataDatetime'].dt.strftime('%Y-%m-%d').unique()):
                    #    processfile(df.loc[df[df_status_datetime[0]] <= d].groupby(file_keys,as_index=False).last(), fn, d)
                except:
                    logger.error('---Error in file: %s' % fn)

            logger.info(".....closing file "+file)
            csvinput.close()

            logger.info(".....archiving file "+file)
            export.archive(df, "", file, export_path, archive_path, logger, createInitial = True)

#
# CSC-289 Students: This is the block of code we need to look at!
#
else: # NOT wStatus
    # !!!
    # !!! Needs to check for existence of schemas before trying to create any tables
    # !!!

    logger.info('=========begin loop===========')

    # loops through each directory and subdirectory of the Informer export path and processes each file.
    for root, subdirs, files in os.walk(export_path):

        # This block processes each Colleague File's folder (ACAC_CREDENTIAL_1001, etc.)
        for subdir in subdirs:
            logger.info('Processing folder ' + subdir + '...')

            # Get all the files in the folder
            filelist = sorted(glob.iglob(os.path.join(root, subdir, '*.csv')), key=os.path.getmtime)

            # This block processes each Colleague File export. These are exported each day as CSV files.
            for i in range(len(filelist)):
                file = os.path.basename( filelist[i] )

                logger.info("Processing file " + file + "...")

                # Reads in csv file then creates an array out of the headers
                try:
                    inputFrame = pd.read_csv(os.path.join(root, subdir, file), encoding='ansi', dtype='str', na_values=None, keep_default_na=False, engine='python')
                    inputFrame = inputFrame.where(pd.notnull(inputFrame), None) # Keep only non-empty rows
                    columnArray = np.asarray(list(inputFrame)) # this is used to select the types and associations later on

                # The most common error has been that there is an error in the Unicode so handle this
                except UnicodeDecodeError as er:
                    logger.error("Error in File: \t %s \n\n Error: %s \n\n" % (file,er))
                    break

                # The following makes a copy of each for the columns in this data frame
                keyListDict = {k: keyList[k] for k in keyList.keys() & columnArray}   # a dictionary of Columns and their keys
                dataTypesDict = {k: dataTypes[k] for k in dataTypes.keys() & columnArray}   # a dictionary of Columns and their types
                dataTypeMVDict = {k: dataTypeMV[k] for k in dataTypeMV.keys() & columnArray}   # a dictionary of Columns and their multi-value types
                elementAssocTypesDict = {k: elementAssocTypes[k] for k in elementAssocTypes.keys() & columnArray}   # a dictionary of Columns and their Element Association Types
                elementAssocNamesDict = {k: elementAssocNames[k] for k in elementAssocNames.keys() & columnArray}   # a dictionary of Columns and their Element Association Names

                # Remove blank entries from the association and multi-value dictionaries (not every field is multi-valued or in an association)
                for key, val in list(elementAssocNamesDict.items()):
                    if val == None:
                        del elementAssocNamesDict[key]
                        del elementAssocTypesDict[key]
                        del dataTypeMVDict[key]

                for key, val in list(keyListDict.items()):
                    if val != 'K':
                        del keyListDict[key]

                # The COLLEAGUE file name is the directory name minus the version number at the end
                sqlName = subdir[:-5]

                # Get a sorted list of a CSV file named as the file with the version added in the archive folder.
                #     Example: For ACAD_CREDENTIALS_1001, look for ACAD_CREDENTIALS_1001.csv in the archive folder.
                # We need to know if this is the first time this file is being processed.
                # If it is not, this file is the shadow copy of the most recent records in the database.
                archive_filelist = sorted(glob.iglob(os.path.join(archive_path, subdir, subdir + '.csv')), key=os.path.getctime)

                # Check if there are files in the archive folder and we are not already processing a 
                #     diff file (and therefore, do not need to create another diff)
                if (len(archive_filelist) > 0) and not diffs:
                    lastarchive_filename = os.path.basename( archive_filelist[-1] )
                    logger.debug("{0} LASTARCHIVE: {1}".format( timestamp(), lastarchive_filename ))
                    archive_file = pd.read_csv( os.path.join(archive_path, subdir, lastarchive_filename), 
                                                encoding='ansi', dtype='str', 
                                                na_values=None, keep_default_na=False, engine='python' )

                    # Create a diff of the current datafram against the existing shadow copy
                    df = export.createDiff( inputFrame, archive_file )
                else:
                    # No previous copy, so use the current dataframe
                    df = inputFrame

                # If there is no DataDatetime column in the current dataframe, add one using the current date
                if 'DataDatetime' in df.columns:
                    pass
                else:
                    df['DataDatetime'] = datetime.datetime.now()

                if writedb:
                    # If there is data in the dataframe, try to write it to the database.
                    # If it fails, break out of the loop that is processing files in this folder.
                    if df.shape[0] > 0:
                        try:
                            logger.debug("{0} SQL_UPDATE: {1} with {2} rows".format( timestamp(), file, df.shape[0] ))

                            export.executeSQL_UPDATE(engine, df, sqlName, keyListDict, dataTypesDict, dataTypeMVDict, elementAssocTypesDict, elementAssocNamesDict, logger)

                            logger.debug("{0} SQL_UPDATE: {1} with {2} rows [DONE]".format( timestamp(), file, df.shape[0] ))
                        except:
                            logger.error('---Error in file: %s the folder will be skipped' % file)
                            break
                    else:
                        logger.debug("{0} SQL_UPDATE: No updated data for {1}".format( timestamp(), file ))
                    
                # Finally, archive the file in the archive folder if their were no exceptions processing the file.
                logger.debug("{0} Archive: {1}".format( timestamp(), file ))

                export.archive(df, subdir, file, export_path, archive_path, logger, diffs = diffs)

                logger.debug("{0} Archive: {1} [DONE]".format( timestamp(), file ))
                    
                logger.info("Processing file " + file + "...[DONE]")

            logger.info('Processing folder ' + subdir + '...[DONE]')
    
logger.info("DONE.")