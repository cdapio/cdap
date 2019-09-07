/*
 * Copyright © 2019 Cask Data, Inc.
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

import DataSourceConfigurer from 'services/datasource/DataSourceConfigurer';
import { apiCreator } from 'services/resource-helper';

let dataSrc = DataSourceConfigurer.getInstance();
const appPath = '/namespaces/system/apps/delta';
const serviceBasepath = `${appPath}/services/DeltaForce`;
const basepath = `${serviceBasepath}/methods/contexts/:context`;
const instancePath = `${basepath}/instance/:id`;

export const MyDeltaApi = {
  list: apiCreator(dataSrc, 'GET', 'REQUEST', `${basepath}/instances`),
  create: apiCreator(dataSrc, 'POST', 'REQUEST', `${basepath}/instances/create`),
  get: apiCreator(dataSrc, 'GET', 'REQUEST', instancePath),
  update: apiCreator(dataSrc, 'PUT', 'REQUEST', instancePath),
  delete: apiCreator(dataSrc, 'DELETE', 'REQUEST', instancePath),

  // mysql handler
  getTables: apiCreator(dataSrc, 'POST', 'REQUEST', `${basepath}/mysql/database/:database/tables`),
  sampleData: apiCreator(
    dataSrc,
    'POST',
    'REQUEST',
    `${basepath}/mysql/database/:database/tables/:table`
  ),
  getDatabaseInfo: apiCreator(dataSrc, 'POST', 'REQUEST', `${basepath}/mysql/database/:database`),

  // Delta service management
  getApp: apiCreator(dataSrc, 'GET', 'REQUEST', `${appPath}`),
  startService: apiCreator(dataSrc, 'POST', 'REQUEST', `${serviceBasepath}/start`),
  stopService: apiCreator(dataSrc, 'POST', 'REQUEST', `${serviceBasepath}/stop`),
  getServiceStatus: apiCreator(dataSrc, 'GET', 'REQUEST', `${serviceBasepath}/status`),
  pollServiceStatus: apiCreator(dataSrc, 'GET', 'POLL', `${serviceBasepath}/status`),
  createApp: apiCreator(dataSrc, 'PUT', 'REQUEST', `${appPath}`),
  ping: apiCreator(dataSrc, 'GET', 'REQUEST', `${serviceBasepath}/methods/health`),
};
