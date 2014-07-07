@echo OFF
REM Application Manager for managing application lifecycle for ResourceSpammer application 
SET APP_JAR_PREFIX=ResourceSpammer

SET APP_NAME=ResourceSpammer
SET FLOW_NAME=CPUSpammerFlow
SET PROCEDURE_NAME=spamProcedure

REM Set the base directory
for %%i in ("%~dp0..\") do (SET APP_HOME=%%~dpi)

REM Set path for curl.exe
SET PATH=%APP_HOME%\..\..\libexec\bin

for /r %APP_HOME%\target %%a in (%APP_JAR_PREFIX%*) do SET JAR_PATH=%%~dpnxa

if %JAR_PATH% == "" (echo "Could not find application jar with name %APP_JAR_PREFIX%" 
                     GOTO :EOF)

REM Process access token
SET ACCESS_TOKEN=
SET ACCESS_TOKEN_FILE=%HOMEPATH%\.continuuity.accesstoken
if exist %ACCESS_TOKEN_FILE% set /p ACCESS_TOKEN=<%ACCESS_TOKEN_FILE%

REM Process Command line
IF "%1" == "start" GOTO START
IF "%1" == "stop" GOTO STOP
IF "%1" == "status" GOTO STATUS
IF "%1" == "deploy" GOTO DEPLOY
GOTO USAGE

:USAGE
echo Application lifecycle management tool
echo Usage: %0 {deploy^|start^|stop^|status}
GOTO :EOF

:DEPLOY
echo Deploying application...
FOR /F %%i IN ('curl -H "Authorization: Bearer %ACCESS_TOKEN%" -X POST -o /dev/null -sL -w %%{http_code} -H "X-Archive-Name: %APP_NAME%" --data-binary @"%JAR_PATH%" http://localhost:10000/v2/apps') DO SET RESPONSE=%%i
IF  %RESPONSE% == 200  (echo Deployed application 
                        GOTO :EOF)

echo Fail to deploy application
GOTO :EOF

:START
CALL :POST %APP_NAME% flows %FLOW_NAME% start
CALL :POST %APP_NAME% procedures %PROCEDURE_NAME% start
GOTO :EOF

:STOP
CALL :POST %APP_NAME% flows %FLOW_NAME% stop
CALL :POST %APP_NAME% procedures %PROCEDURE_NAME% stop
GOTO :EOF

:STATUS
CALL :GET %APP_NAME% flows %FLOW_NAME% status
CALL :GET %APP_NAME% procedures %PROCEDURE_NAME% status
GOTO :EOF

:POST
SET APP=%~1
SET PROGRAM_TYPE=%~2
SET PROGRAM_NAME=%~3
SET ACTION=%~4

echo %ACTION% %PROGRAM_NAME% for application %APP%

FOR /F %%i IN ('curl -H "Authorization: Bearer %ACCESS_TOKEN%" -X POST -o /dev/null -sL -w %%{http_code} http://localhost:10000/v2/apps/%APP%/%PROGRAM_TYPE%/%PROGRAM_NAME%/%ACTION%') DO SET RESPONSE=%%i
IF NOT %RESPONSE% == 200  (
 echo %ACTION% failed 
 GOTO :EOF
)
echo %ACTION% successful
GOTO :EOF

:GET
SET APP=%~1
SET PROGRAM_TYPE=%~2
SET PROGRAM_NAME=%~3
SET ACTION=%~4

echo %ACTION% %PROGRAM_NAME% for application %APP%
curl -H "Authorization: Bearer %ACCESS_TOKEN%" -X GET -sL  http://localhost:10000/v2/apps/%APP%/%PROGRAM_TYPE%/%PROGRAM_NAME%/%ACTION%
echo.
GOTO :EOF
