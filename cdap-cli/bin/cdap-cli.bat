@echo OFF

REM #################################################################################
REM ##
REM ## Copyright (c) 2014-2016 Cask Data, Inc.
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

ECHO:
ECHO [WARN] %0 is deprecated and will be removed in CDAP 5.0. Please use 'cdap cli' for CDAP command line."
ECHO:
ECHO   cdap cli %*
ECHO:
ECHO:

%CDAP_HOME%/bin/cdap.bat cli %*
