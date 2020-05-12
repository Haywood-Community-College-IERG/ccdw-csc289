@ECHO OFF
IF %1.==. (
    SET ServerName=localhost
    ECHO Using %ServerName%
) else (
    if [%1]==[?] (
        GOTO Help   
    )
    SET ServerName=%1
)

ECHO Delete all the tables and views in database on %ServerName%...
SQLCMD -S %ServerName% -i sql\Delete_All_Tables_And_Views.sql

ECHO Now remove the archived files...
IF EXIST ..\archive\import RD /s /q ..\archive\import
MD ..\archive\import\
ECHO Copy base data into data folder
ROBOCOPY ..\basedata ..\data\ /MIR /NJH /NJS /NDL

GOTO End

:Help
ECHO Must specify a server name (e.g. localhost)

:End
