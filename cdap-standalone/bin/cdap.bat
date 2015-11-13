@echo OFF

REM #################################################################################
REM ##
REM ## Copyright © 2014-2015 Cask Data, Inc.
REM ##
REM ## Licensed under the Apache License, Version 2.0 (the "License"); you may not
REM ## use this file except in compliance with the License. You may obtain a copy of
REM ## the License at
REM ##
REM ## http://www.apache.org/licenses/LICENSE-2.0
REM ##
REM ## Unless required by applicable law or agreed to in writing, software
REM ## distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
REM ## WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
REM ## License for the specific language governing permissions and limitations under
REM ## the License.
REM ##
REM #################################################################################

SET ORIGPATH=%cd%
SET CDAP_HOME=%~dp0
SET CDAP_HOME=%CDAP_HOME:~0,-5%
SET JAVACMD=%JAVA_HOME%\bin\java.exe

REM %CDAP_HOME%
SET CLASSPATH=%CDAP_HOME%\lib\*;%CDAP_HOME%\conf\
SET PATH=%PATH%;%CDAP_HOME%\libexec\bin

cd %CDAP_HOME%

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
echo --enable-debug [ ^<port^> ] to connect to a debug port for Standalone CDAP (default port is 5005)
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
echo Resetting Standalone CDAP...
rmdir /S /Q %CDAP_HOME%\logs %CDAP_HOME%\data > NUL 2>&1
echo CDAP reset successfully.
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
if NOT "%line%" == "7" (
  if NOT "%line%" == "8" (
    echo ERROR: Java version not supported. Please install Java 7 or 8 - other versions of Java are not supported.
    GOTO :FINALLY
  )
)
endlocal

REM Check if Node.js is installed
for %%x in (node.exe) do if [%%~$PATH:x]==[] (
  echo Node.js Standalone CDAP requires Node.js but it's either not installed or not in path. Exiting. 1>&2
  GOTO :FINALLY
)

REM Check for Node.js version
setlocal ENABLEDELAYEDEXPANSION
for /f "tokens=* delims= " %%f in ('node -v') do @(
  set line=%%f
)
set line=%line:v=!!%

for /F "delims=.,v tokens=1,2,3" %%a in ('echo %line%') do (
  if %%a LEQ 1 if %%b LEQ 10 (
    echo Node.js version is not supported. We recommend any version of Node.js greater than v0.10.0.
    GOTO :FINALLY
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

mkdir %CDAP_HOME%\logs > NUL 2>&1

REM Log rotation
call:LOG_ROTATE cdap
call:LOG_ROTATE cdap-process
call:LOG_ROTATE cdap-debug

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

start /B %JAVACMD% !DEBUG_OPTIONS! -Dhadoop.security.group.mapping=org.apache.hadoop.security.JniBasedUnixGroupsMappingWithFallback -Dhadoop.home.dir=%CDAP_HOME%\libexec -classpath %CLASSPATH% co.cask.cdap.StandaloneMain >> %CDAP_HOME%\logs\cdap-process.log 2>&1 < NUL
echo Starting Standalone CDAP...

for /F "TOKENS=1,2,*" %%a in ('tasklist /FI "IMAGENAME eq java.exe"') DO SET MyPID=%%b
echo %MyPID% > %~dsp0MyProg.pid
SET lastPid=%MyPID%
attrib +h %~dsp0MyProg.pid >NUL

:SearchLogs
findstr /R /C:".*Failed to start server.*" %CDAP_HOME%\logs\cdap-process.log >NUL 2>&1
if %errorlevel% == 0 GOTO :ServerError

findstr /R /C:"..* started successfully.*" %CDAP_HOME%\logs\cdap-process.log >NUL 2>&1
if not %errorlevel% == 0 GOTO :SearchLogs
if %errorlevel% == 0 GOTO :ServerSuccess
:EndSearchLogs

:ServerError
echo Failed to start, please check logs for more information
GOTO :STOP

:ServerSuccess
echo Standalone CDAP started succesfully.

IF NOT "!DEBUG_OPTIONS!" == "" (
  echo Remote debugger agent started on port !port!.
)
ENDLOCAL

REM Sleep for 5 seconds to wait for Node.js startup
PING 127.0.0.1 -n 6 > NUL 2>&1

for /F "TOKENS=1,2,*" %%a in ('tasklist /FI "IMAGENAME eq node.exe"') DO SET MyNodePID=%%b
echo %MyNodePID% > %~dsp0MyProgNode.pid
attrib +h %~dsp0MyProgNode.pid >NUL
GOTO :FINALLY

:STOP
echo Stopping Standalone CDAP ...
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
for /F "TOKENS=*" %%b in ('dir  /a-d %CDAP_HOME%\logs 2^>NUL ^| find /c "%extension%" 2^>NUL') DO (
  set /a num=%%b
  FOR /L %%i IN (!num!,-1,1) DO (
    set /a prev_num=%%i+1
    rename %CDAP_HOME%\logs\%extension%.%%i %extension%.!prev_num! >NUL 2>NUL
  )
  rename %CDAP_HOME%\logs\%extension% %extension%.1 >NUL 2>NUL
)
endlocal
