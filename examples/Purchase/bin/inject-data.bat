@echo OFF
REM Data inject script

REM Set the base directory
for %%i in ("%~dp0..\") do (SET APP_HOME=%%~dpi)

REM Set path for curl.exe
SET PATH=%PATH%;%APP_HOME%\libexec
SET STREAM=purchaseStream

REM enable delayed expansion so that variables are expanded in the for loop
SETLOCAL ENABLEDELAYEDEXPANSION

REM Process access token
SET ACCESS_TOKEN=
SET ACCESS_TOKEN_FILE=%HOMEPATH%\.continuuity.accesstoken
if exist %ACCESS_TOKEN_FILE% set /p ACCESS_TOKEN=<%ACCESS_TOKEN_FILE%

echo Sending events to %STREAM%...
FOR /F "delims=" %%i IN (%APP_HOME%\resources\purchases.txt) DO ( 
 SET data=%%i
 SET data=!data:"=\"!
 curl -H "Authorization: Bearer %ACCESS_TOKEN%" -sL -X POST --data "!data!" http://localhost:10000/v2/streams/%STREAM%
)
ENDLOCAL

