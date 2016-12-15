:: ##############################################################################
:: ##
:: ## Copyright (c) 2014-2016 Cask Data, Inc.
:: ##
:: ## Licensed under the Apache License, Version 2.0 (the "License"); you may not
:: ## use this file except in compliance with the License. You may obtain a copy
:: ## of the License at
:: ##
:: ## http://www.apache.org/licenses/LICENSE-2.0
:: ##
:: ## Unless required by applicable law or agreed to in writing, software
:: ## distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
:: ## WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
:: ## License for the specific language governing permissions and limitations
:: ## under the License.
:: ##
:: ##############################################################################

@echo OFF
SET APP=%0
SET "ORIG_DIR=%cd%"

REM Double-quotes surround string to include a space character but are not included in string
REM As CDAP_HOME can include a space, any use of it needs to be surrounded in double-quotes
SET "CDAP_HOME=%~dp0"
SET "CDAP_HOME=%CDAP_HOME:~0,-5%"
IF /i NOT "%CDAP_HOME: =%"=="%CDAP_HOME%" (
  echo CDAP_HOME "%CDAP_HOME%"
  echo Contains one or more space characters, will not work correctly, and is not supported.
  echo Exiting. 
  GOTO :FINALLY
)

SET CDAP_VERSION=@@project.version@@

REM Double-quotes surround string to include a space character but are not included in string
REM As JAVACMD can include a space, any use of it needs to be surrounded in double-quotes
SET "JAVACMD=%JAVA_HOME%\bin\java.exe"

SET DEFAULT_DEBUG_PORT=5005
SET "DEFAULT_JVM_OPTS=-Xmx3096m -XX:MaxPermSize=256m"
REM These double-quotes are included in string
SET HADOOP_HOME_OPTS=-Dhadoop.home.dir="%CDAP_HOME%\libexec"
SET SECURITY_OPTS=-Dhadoop.security.group.mapping=org.apache.hadoop.security.JniBasedUnixGroupsMappingWithFallback

REM %CDAP_HOME%
SET "ORIG_PATH=%PATH%"
SET "PATH=%PATH%;%CDAP_HOME%\libexec\bin;%CDAP_HOME%\lib\native"

cd "%CDAP_HOME%"

REM Process command line
IF "%1" == "cli" GOTO CLI
IF "%1" == "sdk" GOTO SDK
IF "%1" == "tx-debugger" GOTO TX_DEBUGGER
REM Process deprecated SDK arguments
IF "%1" == "start" GOTO SDK_DEPRECATED
IF "%1" == "stop" GOTO SDK_DEPRECATED
IF "%1" == "restart" GOTO SDK_DEPRECATED
IF "%1" == "status" GOTO SDK_DEPRECATED
IF "%1" == "reset" GOTO SDK_DEPRECATED
REM Everything else gets usage
GOTO USAGE

:SDK_DEPRECATED
REM Process deprecated SDK arguments
ECHO:
ECHO [WARN] %0 is deprecated and will be removed in CDAP 5.0. Please use 'cdap sdk' for CDAP command line."
ECHO:
ECHO   cdap sdk %*
ECHO:
ECHO:
IF "%1" == "start" GOTO SDK_START
IF "%1" == "stop" GOTO SDK_STOP
IF "%1" == "restart" GOTO SDK_RESTART
IF "%1" == "status" GOTO SDK_STATUS
IF "%1" == "reset" GOTO SDK_RESET
GOTO SDK_USAGE

:SDK
REM Process SDK arguments
IF "%2" == "start" GOTO SDK_START
IF "%2" == "stop" GOTO SDK_STOP
IF "%2" == "restart" GOTO SDK_RESTART
IF "%2" == "status" GOTO SDK_STATUS
IF "%2" == "reset" GOTO SDK_RESET
GOTO SDK_USAGE

:CHECK_WINDOWS
REM Check for 64-bit version of OS. Currently not supporting 32-bit Windows.
IF NOT EXIST "%PROGRAMFILES(X86)%" (
  echo 32-bit Windows operating system is currently not supported.
  GOTO FINALLY
)
GOTO :EOF

:CHECK_JAVA
REM Check for correct setting for JAVA_HOME path
if ["%JAVA_HOME%"] == [] (
  echo ERROR: JAVA_HOME is set to an invalid directory: %JAVA_HOME%
  echo Please set the JAVA_HOME variable in your environment to match the location of your Java installation.
  EXIT /B 1
)

REM Check for Java version
setlocal ENABLEDELAYEDEXPANSION
set /a counter=0
for /f "tokens=* delims= " %%f in ('"%JAVACMD%" -version 2^>^&1') do @(
  if "!counter!"=="0" set line=%%f
  set /a counter+=1
)
set line=%line:*version "1.=!!%
set line=%line:~0,1%
set java_minimum=7
set java_maximum=8
if NOT "%line%" == "%java_minimum%" (
  if NOT "%line%" == "%java_maximum%" (
    echo ERROR: Java version ^(%line%^) is not supported.
    echo Please install Java %java_minimum% or %java_maximum%: other versions of Java are not supported.
    endlocal
    EXIT /B 1
  )
)
endlocal
GOTO :EOF

:CHECK_NODE
REM Check if Node.js is installed
set nodejs_minimum=v4.5.0
for %%x in (node.exe) do if [%%~$PATH:x]==[] (
  echo Standalone CDAP requires Node.js but it is either not installed or not in the path.
  echo We recommend any version of Node.js starting with %nodejs_minimum%.
  GOTO FINALLY
)

REM Check for Node.js version
setlocal ENABLEDELAYEDEXPANSION
for /f "tokens=* delims= " %%f in ('node -v') do @(
  set line=%%f
)
set line=%line:v=!!%

for /F "delims=.,v tokens=1,2,3" %%a in ('echo %line%') do (
  if %%a LSS 1 if %%b LSS 11 if %%c LSS 36 (
    echo Node.js v%line% is not supported. The minimum version supported is %nodejs_minimum%.
    GOTO FINALLY
  ) else (
    echo Node.js version: v%line% 
  )
)
endlocal
GOTO :EOF

:CHECK_PID
REM Checks if there exists a PID that is already running. Alert user and set exit code.
attrib -h %~dsp0MyProg.pid >NUL
if exist %~dsp0MyProg.pid (
  for /F %%i in (%~dsp0MyProg.pid) do (
    setlocal ENABLEDELAYEDEXPANSION
    IF "%%i" == "No" (
      del %~dsp0MyProg.pid
    ) else (
      for /F "TOKENS=2" %%b in ('TASKLIST /FI "PID eq %%i"') DO (
        set lastPid=%%b
      )
      if "!lastPid!" == "%%i" (
        echo CDAP is running as process %%i.
        endlocal
        EXIT /B 1
      ) else (
        REM If process is not running but PID file exists, delete PID file.
        del %~dsp0MyProg.pid
      )
    )
    endlocal
  )
)
attrib +h %~dsp0MyProg.pid >NUL
GOTO :EOF

:CREATE_LOG_DIR
mkdir "%CDAP_HOME%\logs" > NUL 2>&1
GOTO :EOF

:SET_ACCESS_TOKEN
set "auth_file=%HOMEPATH%\.cdap.accesstoken"
REM Check if token-file is provided. If not, use the default file.
set tokenFileProvided=false
for %%a in (%*) do (
  if "%%a" == "--token-file" (
    set tokenFileProvided=true
  )
)

if "%tokenFileProvided%" == "false" if exist %auth_file% (
  set TOKEN_FILE_OPTS=--token-file %auth_file%
)
GOTO :EOF

:TX_DEBUGGER
REM As CLASSPATH can include a space, any use of it needs to be surrounded in double-quotes
REM Double-quotes surround string to include a space character but are not included in string
REM Note the trailing semi-colon so that the last directory entry is interpreted correctly
SET "CLASSPATH=%CDAP_HOME%\lib\*;%CDAP_HOME%\conf\;"
CALL :CHECK_WINDOWS
CALL :CHECK_JAVA
CALL :CREATE_LOG_DIR
CALL :SET_ACCESS_TOKEN

set class=co.cask.cdap.data2.transaction.TransactionManagerDebuggerMain

REM Skip first parameter
for /f "usebackq tokens=1*" %%i in (`echo %*`) DO @ set params=%%j

"%JAVACMD%" %DEFAULT_JVM_OPTS% %HADOOP_HOME_OPTS% %TOKEN_FILE_OPTS% -classpath "%CLASSPATH%" %class% %params%
GOTO FINALLY

:CLI
REM See TX_DEBUGGER for notes on setting CLASSPATH
SET "CLASSPATH=%CDAP_HOME%\libexec\co.cask.cdap.cdap-cli-%CDAP_VERSION%.jar;%CDAP_HOME%\lib\co.cask.cdap.cdap-cli-%CDAP_VERSION%.jar;%CDAP_HOME%\conf\;"
CALL :CHECK_WINDOWS
CALL :CHECK_JAVA

set class=co.cask.cdap.cli.CLIMain

REM Skip first parameter if first parameter is "cli"
set params=%*
IF "%1" == "cli" for /f "usebackq tokens=1*" %%i in (`echo %*`) DO @ set params=%%j

"%JAVACMD%" -classpath "%CLASSPATH%" %class% %params%
GOTO FINALLY

:USAGE
echo:
echo Usage: %APP% ^<command^> [arguments]
echo:
echo   Commands:
echo:
echo     cli         - Starts a CDAP CLI session
echo     sdk         - Sends the arguments to the SDK service
echo     tx-debugger - Sends the arguments to the CDAP transaction debugger
echo:
echo   Get help for a command by executing:
echo:
echo     %APP% ^<command^> --help
echo:
GOTO FINALLY

:SDK_USAGE
echo:
echo Usage: %APP% sdk [ start ^| stop ^| restart ^| status ^| reset ]
echo:
echo Additional options with start or restart:
echo:
echo   --enable-debug [ ^<port^> ] to connect to a debug port for Standalone CDAP (default port is %DEFAULT_DEBUG_PORT%)
echo:
GOTO FINALLY

:SDK_RESET
CALL :CHECK_PID
IF %ERRORLEVEL% == 0 (
  REM Ask for confirmation from user
  CHOICE /C yn /N /M "This deletes all apps, data, and logs. Are you certain you want to proceed? (y/n) "
  IF ERRORLEVEL 2 GOTO :FINALLY
  REM Delete logs and data directories
  echo Resetting Standalone CDAP...
  rmdir /S /Q "%CDAP_HOME%\logs" "%CDAP_HOME%\data" > NUL 2>&1
  echo CDAP reset successfully.
) else (
  echo Stop it first using the 'cdap sdk stop' command.
)
GOTO FINALLY

:SDK_START
CALL :CHECK_PID
IF %ERRORLEVEL% NEQ 0 (
  echo Stop it first using either the 'cdap sdk stop' or 'cdap sdk restart' commands.
  GOTO FINALLY
)
REM See TX_DEBUGGER for notes on setting CLASSPATH
SET "CLASSPATH=%CDAP_HOME%\lib\*;%CDAP_HOME%\conf\;"
CALL :CHECK_WINDOWS
CALL :CHECK_JAVA
IF %ERRORLEVEL% NEQ 0 GOTO FINALLY
CALL :CHECK_NODE
CALL :CREATE_LOG_DIR

REM Log rotation
CALL :LOG_ROTATE cdap
CALL :LOG_ROTATE cdap-process
CALL :LOG_ROTATE cdap-debug

REM Check if debugging is enabled
SET DEBUG_OPTIONS=
SETLOCAL ENABLEDELAYEDEXPANSION
IF "%3" == "--enable-debug" (
  IF "%4" == "" (
    set port=%DEFAULT_DEBUG_PORT%
  ) ELSE (
    REM Check if port is a number
    SET "check="&FOR /f "delims=0123456789" %%i IN ("%4") DO SET check="x"
    IF DEFINED check (
      echo Port number must be an integer.
      ENDLOCAL
      GOTO FINALLY
    )
    REM Check if the number is in range
    set port=%4
    IF !port! LSS 1024 (
      echo Port number must be between 1024 and 65535.
      ENDLOCAL
      GOTO FINALLY
    )
    IF !port! GTR 65535 (
      echo Port number must be between 1024 and 65535.
      ENDLOCAL
      GOTO FINALLY
    )
  )
  set "DEBUG_OPTIONS=-agentlib:jdwp=transport=dt_socket,address=localhost:!port!,server=y,suspend=n"
)
set class=co.cask.cdap.StandaloneMain
REM Note use of an empty title "" in the start command; without it, Windows will
REM mis-interpret the JAVACMD incorrectly if it has spaces in it
start "" /B "%JAVACMD%" %DEFAULT_JVM_OPTS% %HADOOP_HOME_OPTS% !DEBUG_OPTIONS! %SECURITY_OPTS% -classpath "%CLASSPATH%" %class% >> "%CDAP_HOME%\logs\cdap-process.log" 2>&1 < NUL
echo Starting Standalone CDAP...

for /F "TOKENS=1,2,*" %%a in ('tasklist /FI "IMAGENAME eq java.exe"') DO SET MyPID=%%b
echo %MyPID% > %~dsp0MyProg.pid
SET lastPid=%MyPID%
attrib +h %~dsp0MyProg.pid >NUL

:SearchLogs
<nul (SET /p _tmp=.)
findstr /R /C:".*Failed to start server.*" "%CDAP_HOME%\logs\cdap-process.log" >NUL 2>&1
if %errorlevel% == 0 GOTO ServerError

findstr /R /C:"..* started successfully.*" "%CDAP_HOME%\logs\cdap-process.log" >NUL 2>&1
if not %errorlevel% == 0 GOTO SearchLogs
if %errorlevel% == 0 GOTO ServerSuccess
:EndSearchLogs

:ServerError
ENDLOCAL
echo Failed to start: please check logs for more information.
echo See %CDAP_HOME%\logs\cdap-process.log.
CALL :_SDK_STOP 0
GOTO FINALLY

:ServerSuccess
echo:
echo Standalone CDAP started.

IF NOT "!DEBUG_OPTIONS!" == "" (
  echo Remote debugger agent started on port !port!.
)
ENDLOCAL

REM Sleep for 5 seconds to wait for Node.js startup
echo Waiting for Node.js to start...
PING 127.0.0.1 -n 6 > NUL 2>&1

for /F "TOKENS=1,2,*" %%a in ('tasklist /FI "IMAGENAME eq node.exe"') DO SET MyNodePID=%%b
echo %MyNodePID% > %~dsp0MyProgNode.pid
attrib +h %~dsp0MyProgNode.pid >NUL
echo Standalone CDAP started succesfully.
GOTO FINALLY

:SDK_STOP
CALL :_SDK_STOP 1
GOTO FINALLY

:_SDK_STOP
REM Pass a flag as a parameter to show or hide error messages
SET show_error_messages=%1%
SET error_code=0
echo Stopping Standalone CDAP...
attrib -h %~dsp0MyProg.pid >NUL
IF exist %~dsp0MyProg.pid (
  for /F %%i in (%~dsp0MyProg.pid) do (
    IF "%%i" == "No" (
      IF %show_error_messages% EQU 1 echo No valid PID in PID file.    
    ) else (
      taskkill /F /PID %%i >NUL 2>&1
      del %~dsp0MyProg.pid 1>NUL 2>&1
    )
  )
) else (
  IF %show_error_messages% EQU 1 echo No PID file found.
  SET error_code=1
)

REM Sleep for 5 seconds
PING 127.0.0.1 -n 6 > NUL 2>&1

attrib -h %~dsp0MyProgNode.pid >NUL
IF exist %~dsp0MyProgNode.pid (
  for /F %%i in (%~dsp0MyProgNode.pid) do (
    IF "%%i" == "No" (
      IF %show_error_messages% EQU 1 echo No valid PID in Node PID file.
    ) else (
      taskkill /F /PID %%i >NUL 2>&1
      del %~dsp0MyProgNode.pid 1>NUL 2>&1
    )
  )
) else (
  IF %show_error_messages% EQU 1 echo No Node PID file found.
  SET error_code=1
)
IF %error_code% EQU 1 (
  IF %show_error_messages% EQU 1 (
    echo Is CDAP running? Use 'cdap sdk status'.
  )
) else (
  echo Finished stopping Standalone CDAP.
)
GOTO :EOF

:SDK_STATUS
attrib -h %~dsp0MyProg.pid >NUL
if NOT exist %~dsp0MyProg.pid (
  echo CDAP is not running.
) else (
  for /F %%i in (%~dsp0MyProg.pid) do (
    if "%%i" == "No" (
      echo The PID file exists but there is no PID in it.
    ) else (
      setlocal ENABLEDELAYEDEXPANSION
      for /F "TOKENS=2" %%b in ('TASKLIST /FI "PID eq %%i"') DO (
        set lastPid=%%b
      )
      if "!lastPid!" == "%%i" (
        echo CDAP is running as process !lastPid!.
      ) else (
        echo The PID file exists but the process does not appear to be running.
      )
      endlocal
    )
  )
)
attrib +h %~dsp0MyProg.pid >NUL
GOTO FINALLY

:SDK_RESTART
CALL :_SDK_STOP 0
GOTO SDK_START

:LOG_ROTATE
setlocal ENABLEDELAYEDEXPANSION
set extension=%1.log
for /F "TOKENS=*" %%b in ('dir  /a-d "%CDAP_HOME%\logs" 2^>NUL ^| find /c "%extension%" 2^>NUL') DO (
  set /a num=%%b
  FOR /L %%i IN (!num!,-1,1) DO (
    set /a prev_num=%%i+1
    rename "%CDAP_HOME%\logs\%extension%.%%i" %extension%.!prev_num! >NUL 2>NUL
  )
  rename "%CDAP_HOME%\logs\%extension%" %extension%.1 >NUL 2>NUL
)
endlocal
GOTO :EOF

:FINALLY
cd %ORIG_DIR%
SET "PATH=%ORIG_PATH%"
