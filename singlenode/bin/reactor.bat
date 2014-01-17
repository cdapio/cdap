@echo OFF

REM ##############################################################################
REM ##
REM ##  Continuuity Reactor start up script for WINDOWS
REM ##
REM ##############################################################################

setlocal EnableDelayedExpansion
FOR /F "skip=1 tokens=1-6" %%A IN ('WMIC Path Win32_LocalTime Get Day^,Hour^,Minute^,Month^,Second^,Year /Format:table') DO (
    if "%%B" NEQ "" (
        SET /A FDATE=%%F
    )
)

echo ======================================================================================
echo Continuuity Reactor (tm) - Copyright 2012-!FDATE! Continuuity,Inc. All Rights Reserved.
echo ======================================================================================

endlocal

SET CONTINUUITY_HOME=%~dp0
SET CONTINUUITY_HOME=%CONTINUUITY_HOME:~0,-5%
SET JAVACMD=%JAVA_HOME%\bin\java.exe

REM Specifies Web App Path
SET WEB_APP_PATH=%CONTINUUITY_HOME%\web-app\local\server\main.js

REM %CONTINUUITY_HOME%
SET CLASSPATH=%CONTINUUITY_HOME%\lib\*;%CONTINUUITY_HOME%\conf\

cd %CONTINUUITY_HOME%

REM Process command line
IF "%1" == "start" GOTO START
IF "%1" == "stop" GOTO STOP
IF "%1" == "restart" GOTO RESTART
IF "%1" == "status" GOTO STATUS
GOTO USAGE


:USAGE
echo "Usage: %0 {start|stop|restart|status}"
GOTO :EOF


:START
REM Check for correct setting for JAVA_HOME path
if [%JAVA_HOME%] == [] (
  echo ERROR: JAVA_HOME is set to an invalid directory: %JAVA_HOME%
  echo Please set the JAVA_HOME variable in your environment to match the location of your Java installation.
  GOTO :EOF
)

REM Check if Node.js is installed
for %%x in (node.exe) do if [%%~$PATH:x]==[] (
  echo Node.js Continuuity Reactor requires nodeJS but it's either not installed or not in path. Aborting. 1>&2
  GOTO :EOF
)

REM checks if there exists a PID that is already running. Alert user but still return success
attrib -h %~dsp0MyProg.pid >NUL
if exist %~dsp0MyProg.pid (
  for /F %%i in (%~dsp0MyProg.pid) do (
    for /F "TOKENS=2" %%b in ('TASKLIST /FI "PID eq %%i"') DO (
      set lastPid=%%b
    )
    if "%lastPid%" == "%%i" (
      echo %0 running as process %%i. Stop it first or use the restart function.
      GOTO :EOF
    ) else (
      REM If process not running but pid file exists, delete pid file.
      del %~dsp0MyProg.pid
    )
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

start /B %JAVACMD% -Dhadoop.security.group.mapping=org.apache.hadoop.security.JniBasedUnixGroupsMappingWithFallback -Dhadoop.home.dir=%CONTINUUITY_HOME% -Dcontinuuity.home.dir=%CONTINUUITY_HOME% -classpath %CLASSPATH% com.continuuity.SingleNodeMain --web-app-path %WEB_APP_PATH% >> %CONTINUUITY_HOME%\logs\reactor-process.log 2>&1 < NUL
echo Starting Continuuity Reactor ...

for /F "TOKENS=1,2,*" %%a in ('tasklist /FI "IMAGENAME eq java.exe"') DO SET MyPID=%%b
echo %MyPID% > %~dsp0MyProg.pid
attrib +h %~dsp0MyProg.pid >NUL

REM Sleep for 5 seconds to wait for node.Js startup
PING 1.1.1.1 -n 1 -w 5000 > NUL 2>&1
for /F %%i in (%~dsp0MyProg.pid) DO ()

GOTO :EOF


:STOP
echo Stopping Continuuity Reactor ...
attrib -h %~dsp0MyProg.pid >NUL
if exist %~dsp0MyProg.pid (
  for /F %%i in (%~dsp0MyProg.pid) do taskkill /F /PID %%i >NUL
    del %~dsp0MyProg.pid 1>NUL 2>&1
)
GOTO :EOF


:STATUS
attrib -h %~dsp0MyProg.pid >NUL
if NOT exist %~dsp0MyProg.pid (
  echo %0 is not running
) else (
for /F %%i in (%~dsp0MyProg.pid) do (
  for /F "TOKENS=2" %%b in ('TASKLIST /FI "PID eq %%i"') DO (
    set lastPid=%%b
  )
  if "%lastPid%" == "%%i" (
    echo %0 running as process %lastPid%
  ) else (
    echo pidfile exists but process does not appear to be running
  )
 )
)
attrib +h %~dsp0MyProg.pid >NUL
GOTO :EOF


:RESTART
call :STOP
call :START
GOTO :EOF
