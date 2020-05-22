@ECHO OFF
SET PTH="%~dp0"
ECHO %PTH%
"C:\Python\Python38\python.exe" .\import.py --path="%~dp0" %*