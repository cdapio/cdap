/*
 * Copyright © 2018 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import {apiCreator} from 'services/resource-helper';
import DataSourceConfigurer from 'services/datasource/DataSourceConfigurer';
let dataSrc = DataSourceConfigurer.getInstance();

let appPath = '/namespaces/system/apps/ReportGenerationApp';
let programPath = `${appPath}/spark/ReportGenerationSpark`;

let methodsPath = `${programPath}/methods`;
let basepath = `${methodsPath}/reports/:reportId`;

export const MyReportsApi = {
  list: apiCreator(dataSrc, 'GET', 'REQUEST', `${methodsPath}/reports`),
  getDetails: apiCreator(dataSrc, 'GET', 'REQUEST', `${basepath}/details`),
  getReport: apiCreator(dataSrc, 'GET', 'REQUEST', basepath),
  generateReport: apiCreator(dataSrc, 'POST', 'REQUEST', `${methodsPath}/reports`),
  deleteReport: apiCreator(dataSrc, 'DELETE', 'REQUEST', basepath),
  saveReport: apiCreator(dataSrc, 'POST', 'REQUEST', `${basepath}/save`),

  // report service lifecycle
  getApp: apiCreator(dataSrc, 'GET', 'REQUEST', appPath),
  startService: apiCreator(dataSrc, 'POST', 'REQUEST', `${programPath}/start`),
  stopService: apiCreator(dataSrc, 'POST', 'REQUEST', `${programPath}/stop`),
  pollServiceStatus: apiCreator(dataSrc, 'GET', 'POLL', `${programPath}/status`, { interval: 2000 }),
  createApp: apiCreator(dataSrc, 'PUT', 'REQUEST', appPath),
  ping: apiCreator(dataSrc, 'GET', 'REQUEST', `${methodsPath}/reports`),
  deleteApp: apiCreator(dataSrc, 'DELETE', 'REQUEST', appPath)
};
