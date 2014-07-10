@echo OFF

REM ##############################################################################
REM ##
REM ##  Continuuity Reactor start up script for WINDOWS
REM ##  Copyright 2012-2014 Continuuity,Inc. All Rights Reserved.
REM ##
REM ##############################################################################

SET ORIGPATH=%cd%
SET CONTINUUITY_HOME=%~dp0
SET CONTINUUITY_HOME=%CONTINUUITY_HOME:~0,-5%
SET JAVACMD=%JAVA_HOME%\bin\java.exe

REM Specifies Web App Path
SET WEB_APP_PATH=%CONTINUUITY_HOME%\web-app\local\server\main.js

REM %CONTINUUITY_HOME%
SET CLASSPATH=%CONTINUUITY_HOME%\lib\*;%CONTINUUITY_HOME%\conf\
SET PATH=%PATH%;%CONTINUUITY_HOME%\libexec\bin

cd %CONTINUUITY_HOME%



REM Get app jar with latest version
SET APP_JAR_PREFIX=ResponseCodeAnalytics
for /r %CONTINUUITY_HOME%\examples\%APP_JAR_PREFIX%\target %%a in (%APP_JAR_PREFIX%*) do SET JAR_PATH=%%~dpnxa

if %JAR_PATH% == "" echo "Could not find example application jar with name %APP_JAR_PREFIX%"


REM Process command line
IF "%1" == "start" GOTO START
IF "%1" == "stop" GOTO STOP
IF "%1" == "restart" GOTO RESTART
IF "%1" == "status" GOTO STATUS
IF "%1" == "reset" GOTO RESET
GOTO USAGE


:USAGE
echo Usage: %0 {start^|stop^|restart^|status^|reset}
echo Additional options with start, restart:
echo --enable-debug [ ^<port^> ] to connect to a debug port for local reactor (default port is 5005)
GOTO :FINALLY

:RESET
REM checks if there exists a PID that is already running. Alert user but still return success
attrib -h %~dsp0MyProg.pid >NUL
if exist %~dsp0MyProg.pid (
  for /F %%i in (%~dsp0MyProg.pid) do (
    for /F "TOKENS=2" %%b in ('TASKLIST /FI "PID eq %%i"') DO (
      set lastPid=%%b
    )
    if "%lastPid%" == "%%i" (
      echo %0 running as process %%i. Stop it first.
      GOTO :FINALLY
    ) else (
      REM If process not running but pid file exists, delete pid file.
      del %~dsp0MyProg.pid
    )
  )
)
attrib +h %~dsp0MyProg.pid >NUL

REM ask for confirmation from user
set /P answer="This deletes all apps, data and logs. Are you sure you want to proceed? (y/n) " %=%
if NOT "%answer%" == "y" (
  GOTO :FINALLY
)

REM delete logs and data directories
echo Resetting Continuuity Reactor ...
rmdir /S /Q %CONTINUUITY_HOME%\logs %CONTINUUITY_HOME%\data > NUL 2>&1
echo Continuuity Reactor reset successfully.
GOTO :FINALLY


:START
REM Check for 64-bit version of OS. Currently not supporting 32-bit Windows
IF NOT EXIST "%PROGRAMFILES(X86)%" (
  echo 32-bit Windows operating system is currently not supported
  GOTO :FINALLY
)

REM Check for correct setting for JAVA_HOME path
if [%JAVA_HOME%] == [] (
  echo ERROR: JAVA_HOME is set to an invalid directory: %JAVA_HOME%
  echo Please set the JAVA_HOME variable in your environment to match the location of your Java installation.
  GOTO :FINALLY
)

REM Check for Java version
setlocal ENABLEDELAYEDEXPANSION
set /a counter=0
for /f "tokens=* delims= " %%f in ('%JAVACMD% -version 2^>^&1') do @(
  if "!counter!"=="0" set line=%%f
  set /a counter+=1
)
set line=%line:java version "1.=!!%
set line=%line:~0,1%
if NOT "%line%" == "6" (
  if NOT "%line%" == "7" (
    echo ERROR: Java version not supported. Please install Java 6 or 7 - other versions of Java are not yet supported.
    GOTO :FINALLY
  )
)
endlocal

REM Check if Node.js is installed
for %%x in (node.exe) do if [%%~$PATH:x]==[] (
  echo Node.js Continuuity Reactor requires nodeJS but it's either not installed or not in path. Aborting. 1>&2
  GOTO :FINALLY
)

REM Check for Node.js version
setlocal ENABLEDELAYEDEXPANSION
for /f "tokens=* delims= " %%f in ('node -v') do @(
  set line=%%f
)
set line=%line:v=!!%
set line=0.10.1

for /F "delims=. tokens=1,2,3" %%a in ('echo %line%') do (
  if %%a LSS 1 (
    if %%b LSS 9 (
      if %%c LSS 16 (
        echo Node.js version is not supported. The minimum version suported is v0.8.16.
        GOTO :FINALLY
      )
    )
  )
)
endlocal

REM checks if there exists a PID that is already running. Alert user but still return success
attrib -h %~dsp0MyProg.pid >NUL
if exist %~dsp0MyProg.pid (
  for /F %%i in (%~dsp0MyProg.pid) do (
    setlocal ENABLEDELAYEDEXPANSION
    for /F "TOKENS=2" %%b in ('TASKLIST /FI "PID eq %%i"') DO (
      set lastPid=%%b
    )
    if "!lastPid!" == "%%i" (
      echo %0 running as process %%i. Stop it first or use the restart function.
      GOTO :FINALLY
    ) else (
      REM If process not running but pid file exists, delete pid file.
      del %~dsp0MyProg.pid
    )
    endlocal
  )
)
attrib +h %~dsp0MyProg.pid >NUL

REM Check for new version of Reactor
bitsadmin /Transfer NAME http://www.continuuity.com/version %~f0_version.txt > NUL 2>&1
if exist %~f0_version.txt (
  for /f "tokens=* delims= " %%f in (%~f0_version.txt) do (
    SET new_version = %%f
  )
  for /f "tokens=* delims= " %%g in (%~f0\..\..\VERSION) do (
    SET current_version = %%g
  )
  del %~f0_version.txt > NUL 2>&1

  if not "%current_version%" == "%new_version%" (
    echo UPDATE: There is a newer version of Continuuity Developer Suite available.
    echo         Download it from your account: https://accounts.continuuity.com.
  )
)

mkdir %CONTINUUITY_HOME%\logs > NUL 2>&1

REM Log rotation
call:LOG_ROTATE reactor
call:LOG_ROTATE reactor-process
call:LOG_ROTATE reactor-debug

REM check if debugging is enabled
SET DEBUG_OPTIONS=
setlocal ENABLEDELAYEDEXPANSION
IF "%2" == "--enable-debug" (
  IF "%3" == "" (
    set port=5005
  ) ELSE (
    REM check if port is a number
    SET "check="&FOR /f "delims=0123456789" %%i IN ("%3") DO SET check="x"
    IF DEFINED check (
      echo port number must be an integer.
      ENDLOCAL
      GOTO :FINALLY
    )
    REM check if the number is in range
    set port=%3
    IF !port! LSS 1024 (
      echo port number must be between 1024 and 65535.
      ENDLOCAL
      GOTO :FINALLY
    )
    IF !port! GTR 65535 (
      echo port number must be between 1024 and 65535.
      ENDLOCAL
      GOTO :FINALLY
    )
  )
  set DEBUG_OPTIONS="-agentlib:jdwp=transport=dt_socket,address=localhost:!port!,server=y,suspend=n"
)

start /B %JAVACMD% !DEBUG_OPTIONS! -Dhadoop.security.group.mapping=org.apache.hadoop.security.JniBasedUnixGroupsMappingWithFallback -Dhadoop.home.dir=%CONTINUUITY_HOME%\libexec -classpath %CLASSPATH% com.continuuity.SingleNodeMain --web-app-path %WEB_APP_PATH% >> %CONTINUUITY_HOME%\logs\reactor-process.log 2>&1 < NUL
echo Starting Continuuity Reactor ...

for /F "TOKENS=1,2,*" %%a in ('tasklist /FI "IMAGENAME eq java.exe"') DO SET MyPID=%%b
echo %MyPID% > %~dsp0MyProg.pid
SET lastPid=%MyPID%
attrib +h %~dsp0MyProg.pid >NUL

:SearchLogs
findstr /R /C:".*Failed to start server.*" %CONTINUUITY_HOME%\logs\reactor-process.log >NUL 2>&1
if %errorlevel% == 0 GOTO :ServerError

findstr /R /C:".*Continuuity Reactor started successfully.*" %CONTINUUITY_HOME%\logs\reactor-process.log >NUL 2>&1
if not %errorlevel% == 0 GOTO :SearchLogs
if %errorlevel% == 0 GOTO :ServerSuccess
:EndSearchLogs

:ServerError
echo Failed to start, please check logs for more information
GOTO :STOP

:ServerSuccess
echo Reactor started succesfully.

IF NOT "!DEBUG_OPTIONS!" == "" (
  echo Remote debugger agent started on port !port!.
)
ENDLOCAL

REM Sleep for 5 seconds to wait for node.Js startup
PING 127.0.0.1 -n 6 > NUL 2>&1

for /F "TOKENS=1,2,*" %%a in ('tasklist /FI "IMAGENAME eq node.exe"') DO SET MyNodePID=%%b
echo %MyNodePID% > %~dsp0MyProgNode.pid
attrib +h %~dsp0MyProgNode.pid >NUL


CALL :NUX
GOTO :FINALLY

:NUX
REM New user experience, enable if it is not enabled already
if not exist %CONTINUUITY_HOME%\.nux_dashboard (

  REM Deploy app
  FOR /F %%i IN ('curl -X POST -sL -w %%{http_code} -H "X-Archive-Name: LogAnalytics.jar" --data-binary @"%JAR_PATH%" http://127.0.0.1:10000/v2/apps') DO SET RESPONSE=%%i
  REM IF  NOT %RESPONSE% == 200  (GOTO :EOF)
  REM Start flow
  curl -sL -X POST http://127.0.0.1:10000/v2/apps/ResponseCodeAnalytics/flows/LogAnalyticsFlow/start
  REM Start procedure
  curl -sL -X POST http://127.0.0.1:10000/v2/apps/ResponseCodeAnalytics/procedures/StatusCodeProcedure/start
)
GOTO :EOF

:STOP
echo Stopping Continuuity Reactor ...
attrib -h %~dsp0MyProg.pid >NUL
if exist %~dsp0MyProg.pid (
  for /F %%i in (%~dsp0MyProg.pid) do (
    taskkill /F /PID %%i >NUL 2>&1
    del %~dsp0MyProg.pid 1>NUL 2>&1
  )
)

REM Sleep for 5 seconds
PING 127.0.0.1 -n 6 > NUL 2>&1

attrib -h %~dsp0MyProgNode.pid >NUL
if exist %~dsp0MyProgNode.pid (
  for /F %%i in (%~dsp0MyProgNode.pid) do (
    taskkill /F /PID %%i >NUL 2>&1
    del %~dsp0MyProgNode.pid 1>NUL 2>&1
  )
)
GOTO :FINALLY


:STATUS
attrib -h %~dsp0MyProg.pid >NUL
if NOT exist %~dsp0MyProg.pid (
  echo %0 is not running
) else (
for /F %%i in (%~dsp0MyProg.pid) do (
  setlocal ENABLEDELAYEDEXPANSION
  for /F "TOKENS=2" %%b in ('TASKLIST /FI "PID eq %%i"') DO (
    set lastPid=%%b
  )
  if "!lastPid!" == "%%i" (
    echo %0 running as process !lastPid!
  ) else (
    echo pidfile exists but process does not appear to be running
  )
  endlocal
 )
)
attrib +h %~dsp0MyProg.pid >NUL
GOTO :FINALLY


:RESTART
CALL :STOP
GOTO :START

:FINALLY
cd %ORIGPATH%
GOTO:EOF


:LOG_ROTATE
setlocal ENABLEDELAYEDEXPANSION
set extension=%1.log
for /F "TOKENS=*" %%b in ('dir  /a-d %CONTINUUITY_HOME%\logs 2^>NUL ^| find /c "%extension%" 2^>NUL') DO (
  set /a num=%%b
  FOR /L %%i IN (!num!,-1,1) DO (
    set /a prev_num=%%i+1
    rename %CONTINUUITY_HOME%\logs\%extension%.%%i %extension%.!prev_num! >NUL 2>NUL
  )
  rename %CONTINUUITY_HOME%\logs\%extension% %extension%.1 >NUL 2>NUL
)
endlocal
