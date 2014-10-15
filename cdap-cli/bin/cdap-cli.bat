@echo OFF

REM #################################################################################
REM ##
REM ## Copyright © 2014 Cask Data, Inc.
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

SET CDAP_HOME=%~dp0
SET CDAP_HOME=%CDAP_HOME:~0,-5%
SET LIB=%CDAP_HOME%\lib
SET JAVACMD=%JAVA_HOME%\bin\java.exe

SET CLASSPATH=%CDAP_HOME%\conf\;%LIB%\aopalliance.aopalliance-1.0.jar;%LIB%\co.cask.cdap.cdap-api-2.5.1.jar;%LIB%\co.cask.cdap.cdap-authentication-client-1.0.1.jar;%LIB%\co.cask.cdap.cdap-cli-2.5.1.jar;%LIB%\co.cask.cdap.cdap-client-2.5.1.jar;%LIB%\co.cask.cdap.cdap-common-2.5.1.jar;%LIB%\co.cask.cdap.cdap-proto-2.5.1.jar;%LIB%\co.cask.tephra.tephra-api-0.3.0.jar;%LIB%\com.google.code.findbugs.jsr305-2.0.1.jar;%LIB%\com.google.code.gson.gson-2.2.4.jar;%LIB%\com.google.guava.guava-13.0.1.jar;%LIB%\com.google.inject.extensions.guice-assistedinject-3.0.jar;%LIB%\com.google.inject.extensions.guice-multibindings-3.0.jar;%LIB%\com.google.inject.guice-3.0.jar;%LIB%\com.googlecode.concurrent-trees.concurrent-trees-2.4.0.jar;%LIB%\commons-codec.commons-codec-1.6.jar;%LIB%\commons-logging.commons-logging-1.1.1.jar;%LIB%\javax.inject.javax.inject-1.jar;%LIB%\javax.ws.rs.javax.ws.rs-api-2.0.jar;%LIB%\jline.jline-2.12.jar;%LIB%\org.apache.httpcomponents.httpclient-4.2.5.jar;%LIB%\org.apache.httpcomponents.httpcore-4.2.5.jar;%LIB%\org.slf4j.slf4j-api-1.7.5.jar;%LIB%\org.slf4j.slf4j-nop-1.7.5.jar

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

%JAVACMD% -classpath %CLASSPATH% co.cask.cdap.shell.CLIMain %*

:FINALLY
