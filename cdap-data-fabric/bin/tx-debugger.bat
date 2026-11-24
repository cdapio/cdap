:: ##############################################################################
:: ##
:: ## Copyright (c) 2014-2018 Cask Data, Inc.
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
SET CDAP_HOME=%~dp0
SET CDAP_HOME=%CDAP_HOME:~0,-5%
IF /i NOT "%CDAP_HOME: =%"=="%CDAP_HOME%" (
  echo CDAP_HOME "%CDAP_HOME%"
  echo Contains one or more space characters, will not work correctly, and is not supported.
  echo Exiting.
  GOTO :EOF
)

SET JAVACMD=%JAVA_HOME%\bin\java.exe
SET DEFAULT_JVM_OPTS=-Xmx2048m
SET HADOOP_HOME_OPTS=-Dhadoop.home.dir=%CDAP_HOME%\libexec

SET CLASSPATH=%CDAP_HOME%\lib\*;%CDAP_HOME%\conf\
SET "ORIG_PATH=%PATH%"
SET "PATH=%PATH%;%CDAP_HOME%\libexec\bin;%CDAP_HOME%\lib\native"

REM Check for 64-bit version of OS. Currently not supporting 32-bit Windows
IF NOT EXIST "%PROGRAMFILES(X86)%" (
  echo 32-bit Windows operating system is currently not supported
  GOTO FINALLY
)

REM Check for correct setting for JAVA_HOME path
if [%JAVA_HOME%] == [] (
  echo ERROR: JAVA_HOME is set to an invalid directory: %JAVA_HOME%
  echo Please set the JAVA_HOME variable in your environment to match the location of your Java installation.
  GOTO FINALLY
)

REM Check for Java version
setlocal ENABLEDELAYEDEXPANSION
REM Use the first line of the form: ... version "<ver>" ...
REM Lines such as "Picked up JAVA_TOOL_OPTIONS: ..." can come first and are skipped
set "ver="
for /f "tokens=* delims= " %%f in ('"%JAVACMD%" -version 2^>^&1') do @(
  if not defined ver (
    for /f tokens^=1^,2^ delims^=^" %%a in ("%%f") do (
      set "head=%%a"
      if "!head:~-8!"=="version " set "ver=%%b"
    )
  )
)
set "java_major="
for /f "tokens=1,2 delims=.-_+" %%a in ("!ver!") do (
  if "%%a"=="1" (
    set "java_major=%%b"
  ) else (
    set "java_major=%%a"
  )
)
set java_minimum=11
REM LSS falls back to a string compare for non-numbers, so check for digits first
set "non_digit="
if "!java_major!"=="" set "non_digit=x"
for /f "delims=0123456789" %%i in ("!java_major!") do set "non_digit=%%i"
if defined non_digit (
  echo ERROR: Could not detect Java version from ^(!ver!^).
  endlocal
  GOTO JAVA_CHECK_FAILED
)
if !java_major! LSS %java_minimum% (
  echo ERROR: Java version ^(!ver!^) is not supported.
  echo Please install Java %java_minimum% or higher: older versions of Java are not supported.
  endlocal
  GOTO JAVA_CHECK_FAILED
)
endlocal

mkdir %CDAP_HOME%\logs > NUL 2>&1

set auth_file=%HOMEPATH%\.cdap.accesstoken
REM check if token-file is provided. if not use the default file
set tokenFileProvided=false
for %%a in (%*) do (
  if "%%a" == "--token-file" (
    set tokenFileProvided=true
  )
)

if "%tokenFileProvided%" == "false" if exist %auth_file% (
  set TOKEN_FILE_OPTS=--token-file %auth_file%
)

%JAVACMD% %DEFAULT_JVM_OPTS% %HADOOP_HOME_OPTS% %TOKEN_FILE_OPTS% -classpath %CLASSPATH% io.cdap.cdap.data2.transaction.TransactionManagerDebuggerMain %*

:FINALLY
SET "PATH=%ORIG_PATH%"
GOTO :EOF

REM Restores PATH and exits with 1 when the Java check fails
:JAVA_CHECK_FAILED
SET "PATH=%ORIG_PATH%"
EXIT /B 1
