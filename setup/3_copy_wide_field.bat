@ECHO OFF
COPY ..\changed_data\clean\NON_COURSES_1001\*.* ..\data\NON_COURSES_1001
IF EXIST ..\archive\import\NON_COURSES_1001 RD /s /q ..\archive\import\NON_COURSES_1001