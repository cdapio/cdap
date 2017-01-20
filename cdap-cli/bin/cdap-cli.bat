:: ##############################################################################
:: ##
:: ## Copyright (c) 2014-2017 Cask Data, Inc.
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

ECHO:
ECHO [WARN] %0 is deprecated and will be removed in CDAP 5.0. Please use 'cdap cli' for the CDAP command line.
ECHO:
ECHO   cdap cli %*
ECHO:
ECHO:

"%CDAP_HOME%\bin\cdap.bat" cli %*
