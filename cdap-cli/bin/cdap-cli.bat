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
SET "CDAP_HOME=%~dp0"
SET "CDAP_HOME=%CDAP_HOME:~0,-5%"
IF /i NOT "%CDAP_HOME: =%"=="%CDAP_HOME%" (
  echo CDAP_HOME "%CDAP_HOME%"
  echo Contains one or more space characters, will not work correctly, and is not supported.
  echo Exiting. 
  GOTO :FINALLY
)

SET "JAVACMD=%JAVA_HOME%\bin\java.exe"
SET CDAP_VERSION=@@project.version@@

SET "CLASSPATH=%CDAP_HOME%\libexec\co.cask.cdap.cdap-cli-%CDAP_VERSION%.jar;%CDAP_HOME%\lib\co.cask.cdap.cdap-cli-%CDAP_VERSION%.jar;%CDAP_HOME%\conf\;"

REM Check for 64-bit version of OS. Currently not supporting 32-bit Windows
IF NOT EXIST "%PROGRAMFILES(X86)%" (
  echo 32-bit Windows operating system is currently not supported
  GOTO :FINALLY
)

REM Check for correct setting for JAVA_HOME path
if "%JAVA_HOME%" == "" (
  echo ERROR: JAVA_HOME is set to an invalid directory: %JAVA_HOME%
  echo Please set the JAVA_HOME variable in your environment to match the location of your Java installation.
  GOTO :FINALLY
)

REM Check for Java version
setlocal ENABLEDELAYEDEXPANSION
set /a counter=0
for /f "tokens=* delims= " %%f in ('"%JAVACMD%" -version 2^>^&1') do @(
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

"%JAVACMD%" -classpath "%CLASSPATH%" co.cask.cdap.cli.CLIMain %*

:FINALLY
