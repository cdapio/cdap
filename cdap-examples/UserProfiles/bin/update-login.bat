@echo OFF

REM ############################################################################
REM ## Copyright (C) 2016 Cask Data, Inc.
REM ##
REM ## Licensed under the Apache License, Version 2.0 (the "License"); you may
REM ## not use this file except in compliance with the License. You may obtain a
REM ## copy of the License at
REM ##
REM ## http://www.apache.org/licenses/LICENSE-2.0
REM ##
REM ## Unless required by applicable law or agreed to in writing, software
REM ## distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
REM ## WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
REM ## License for the specific language governing permissions and limitations
REM ## under the License.
REM ##
REM ############################################################################

REM Set the base directory
for %%i in ("%~dp0..\") do (SET $APP_HOME=%%~dpi)

REM Set path for curl.exe
set $PATH=%PATH%;%APP_HOME%\..\..\libexec\bin

REM Process access token
set $ACCESS_TOKEN=
set $ACCESS_TOKEN_FILE=%HOMEPATH%\.cdap.accesstoken
if exist %$ACCESS_TOKEN_FILE% set /p $ACCESS_TOKEN=<%$ACCESS_TOKEN_FILE%

set $TABLE=profiles
set $ENDPOINT=v3/namespaces/default/apps/UserProfiles/services/UserProfileService/methods/%$TABLE%

SETLOCAL EnableDelayedExpansion

REM Set parameters
set $SECONDS=%1
set $DELAY=%2
set $HOST=%3
set $ERROR=

if not DEFINED $SECONDS set $ERROR=Seconds must be set
if DEFINED $ERROR goto :USAGE

if not DEFINED $DELAY set $ERROR=Delay must be set
if DEFINED $ERROR goto :USAGE

goto :PROGRAM

:USAGE
SET $PROGRAM_NAME=%0 
echo Tool for updating user's last login time randomly
echo Usage: %$PROGRAM_NAME% seconds delay [host]
echo:
echo Options
echo     seconds   How many seconds to run
echo     delay     How many (integer) seconds to sleep between each event-call (min 1)
echo     host      Specifies the host that CDAP is running on (default: localhost)
echo:
if DEFINED $ERROR echo Error: !$ERROR!
set $ERROR=
goto :FINALLY

:PROGRAM
set $MAX_EVENTS=2147483647
if DEFINED $HOST set $GATEWAY=!$HOST!
if not DEFINED $GATEWAY set $GATEWAY=localhost

REM Populate the "array" $USERS_ARRAY, indexed from 1
REM The commented out inner loop shows how to access it
set /a "$USERS_ARRAY_COUNT=0"
FOR /F "tokens=*" %%G IN (!$APP_HOME!resources\users.txt) DO (
    set /a "$USERS_ARRAY_COUNT+=1"
    set $BODY=%%G
    set $BODY=!$BODY:"='!
    for /F "tokens=1 delims=," %%H IN ('echo !$BODY!') DO set $USERID=%%H
    set $USERID=!$USERID:~9,-1!
    set $USERS_ARRAY[!$USERS_ARRAY_COUNT!]=!$USERID!
    REM for /L %%n in (!$USERS_ARRAY_COUNT!,1,!$USERS_ARRAY_COUNT!) do ( 
    REM     echo !$USERS_ARRAY[%%n]!
    REM )
)

call :GET_EPOCH $START_TIME
set /a "$END_TIME=!$START_TIME!+!$SECONDS!"

echo Randomly updating user's last login time for !$SECONDS! seconds with delay !$DELAY! to the table '!$TABLE!' at !$GATEWAY!

for /L %%G IN (1 1 %$MAX_EVENTS%) DO (
    call :GET_EPOCH $CURRENT_TIME
    if !$CURRENT_TIME! GTR !$END_TIME! goto :FINALLY
    
    set /a "$RID=(!RANDOM!*!$USERS_ARRAY_COUNT!/32768)+1"
    for /L %%n in (!$RID!,1,!$RID!) do ( 
        set $USERID=!$USERS_ARRAY[%%n]!
    )
    for /f %%a in ('copy /Z "%~f0" nul') do set "CR=%%a"
    REM Creates a carriage return (CR) character so that the set will overwrite itself
    set /P "=Setting userID !$USERID! to !$CURRENT_TIME! (press CTRL+C to cancel)!CR!" <nul

    set $AUTH=
    if DEFINED $ACCESS_TOKEN set $AUTH=-H "Authorization: Bearer !$ACCESS_TOKEN!"
    set $CURLX=-X PUT -d!$CURRENT_TIME!
    set $URL="http://!$GATEWAY!:11015/%$ENDPOINT%/!$USERID!/lastLogin"
    set $COMMAND=curl -qfsw "%%{http_code}" !$AUTH! !$CURLX! !$URL!
    set $EXPECTED=200
    for /F "tokens=* USEBACKQ" %%F IN (`!$COMMAND!`) DO (
        set $RESULTS=%%F
    )
    if /I not "!$RESULTS!"=="!$EXPECTED!" echo Failed to update login time for user !$USERID!: return code !$RESULTS!

    set /a $PING_DELAY=!$DELAY!+1
    ping -n !$PING_DELAY! localhost >nul
)
goto :FINALLY

:GET_EPOCH
for /f "delims=" %%x in ('cscript /nologo %$APP_HOME%\bin\to-epoch.vbs') do set "%~1=%%x"
goto :eof

:FINALLY
