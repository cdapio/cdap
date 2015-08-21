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

REM Data inject script

REM Set the base directory
for %%i in ("%~dp0..\") do (SET APP_HOME=%%~dpi)

REM Set path for curl.exe
SET PATH=%PATH%;%APP_HOME%\libexec
SET STREAM=pointsStream

REM enable delayed expansion so that variables are expanded in the for loop
SETLOCAL ENABLEDELAYEDEXPANSION

REM Process access token
SET ACCESS_TOKEN=
SET ACCESS_TOKEN_FILE=%HOMEPATH%\.cdap.accesstoken
if exist %ACCESS_TOKEN_FILE% set /p ACCESS_TOKEN=<%ACCESS_TOKEN_FILE%

echo Sending events to %STREAM%...
FOR /F "delims=" %%i IN (%APP_HOME%\resources\points.txt) DO (
 SET data=%%i
 SET data=!data:"=\"!
 curl -H "Authorization: Bearer %ACCESS_TOKEN%" -sL -X POST --data "!data!" http://localhost:10000/v3/namespaces/default/streams/%STREAM%
)
ENDLOCAL

