@echo OFF
REM Data inject script

REM Set the base directory
for %%i in ("%~dp0..\") do (SET APP_HOME=%%~dpi)

REM Set path for curl.exe
SET PATH=%PATH%;%APP_HOME%\libexec
SET STREAM=logEventStream

REM enable delayed expansion so that variables are expanded in the for loop
SETLOCAL ENABLEDELAYEDEXPANSION

echo Sending events to %STREAM%...
FOR /F "delims=" %%i IN (%APP_HOME%\resources\apache.accesslog) DO ( 
 SET data=%%i
 SET data=!data:"=\"!
 curl -sL -X POST --data "!data!" http://localhost:10000/v2/streams/%STREAM% 
)
ENDLOCAL

