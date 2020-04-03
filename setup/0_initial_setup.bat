@ECHO OFF
REM Need server name as first argument

IF %1.==. (
    SET ServerName=localhost
    ECHO Using %ServerName%
) else (
    if [%1]==[?] (
        GOTO Help   
    )
    SET ServerName=%1
)

ECHO Setting up initial database
SQLCMD -S %ServerName% -Q "DROP DATABASE CCDW"
SQLCMD -S %ServerName% -Q "CREATE DATABASE CCDW"
SQLCMD -S %ServerName% -i sql\Create_Schemas.sql > NUL
SQLCMD -S %ServerName% -i sql\dbo.DelimitedSplit8K.sql > NUL
SQLCMD -S %ServerName% -i sql\dw_util.GetEasterHolidays.sql > NUL
SQLCMD -S %ServerName% -i sql\dw_util.Update_Date.sql > NUL
SQLCMD -S %ServerName% -i sql\RestoreDB2Base.sql > NUL
SQLCMD -S %ServerName% -i sql\RestoreDB2Empty.sql > NUL

ECHO Removing old data files
REM Remove all folders from data
RD /q /s ..\data 2> NUL

ECHO Removing old archive files
REM Remove all folders from archive
RD /q /s ..\archive 2> NUL

ECHO Recreate the necessary folders
CALL .\Create_Folder.bat 

ECHO Copy basedata for CSC289...
ROBOCOPY ..\basedata ..\data /MIR /NFL /NDL /NJH /NJS /NC /NS /NP > NUL 1>&2

ECHO Now call startImport...
CD ..\import
CALL .\startImport.bat 

CD ..\setup

ECHO Final database setup to allow restoring database to base version
SQLCMD -S %ServerName% -i sql\RestoreDB2Base_Creation.sql > NUL

GOTO End

:Help
ECHO Must specify a server name (e.g. localhost)

:End