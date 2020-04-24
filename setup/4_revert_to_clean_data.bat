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

ECHO Restoring initial database on %ServerName%...
SQLCMD -S %ServerName% -Q "EXEC CCDW.dbo.RestoreDB2Base"

ECHO Now remove the archived files...
IF EXIST ..\archive\import\COURSE_SECTIONS_1002 RD /s /q ..\archive\import\COURSE_SECTIONS_1002
IF EXIST ..\archive\import\COURSE_SECTIONS_1003 RD /s /q ..\archive\import\COURSE_SECTIONS_1003
IF EXIST ..\archive\import\NON_COURSES_1001 RD /s /q ..\archive\import\NON_COURSES_1001
MD ..\archive\import\NON_COURSES_1001
COPY ..\basedata\NON_COURSES_1001\NON_COURSES_1001_Initial.csv ..\archive\import\NON_COURSES_1001\NON_COURSES_1001.csv
COPY ..\basedata\NON_COURSES_1001\NON_COURSES_1001_Initial.csv ..\archive\import\NON_COURSES_1001\NON_COURSES_1001_Initial.csv

GOTO End

:Help
ECHO Must specify a server name (e.g. localhost)

:End
