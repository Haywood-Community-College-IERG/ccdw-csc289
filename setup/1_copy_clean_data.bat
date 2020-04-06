@ECHO OFF
COPY ..\changed_data\clean\ACAD_PROGRAMS_1001\*.* ..\data\ACAD_PROGRAMS_1001
IF EXIST ..\archive\import\ACAD_PROGRAMS_1001 RD /s /q ..\archive\import\ACAD_PROGRAMS_1001