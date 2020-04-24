@ECHO OFF

ECHO Restore the archive folder for NON_COURSES_1001...
COPY ..\changed_data\wide_field\NON_COURSES_1001\*.* ..\data\NON_COURSES_1001
IF EXIST ..\archive\import\NON_COURSES_1001 RD /s /q ..\archive\import\NON_COURSES_1001
MD ..\archive\import\NON_COURSES_1001
COPY ..\basedata\NON_COURSES_1001\NON_COURSES_1001_Initial.csv ..\archive\import\NON_COURSES_1001\NON_COURSES_1001.csv
COPY ..\basedata\NON_COURSES_1001\NON_COURSES_1001_Initial.csv ..\archive\import\NON_COURSES_1001\NON_COURSES_1001_Initial.csv