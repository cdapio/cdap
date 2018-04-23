/*
 * Copyright © 2017-2018 Cask Data, Inc.
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
import {apiCreator} from 'services/resource-helper';

let dataSrc = DataSourceConfigurer.getInstance();
let appPath = '/namespaces/:namespace/apps/ModelManagementApp';
let servicePath = `${appPath}/spark/ModelManagerService`;
let basePath = `${servicePath}/methods`;

export const myExperimentsApi = {
  list: apiCreator(dataSrc, 'GET', 'REQUEST', `${basePath}/experiments`),

  getExperiment: apiCreator(dataSrc, 'GET', 'REQUEST', `${basePath}/experiments/:experimentId`),
  getModelsInExperiment: apiCreator(dataSrc, 'GET', 'REQUEST', `${basePath}/experiments/:experimentId/models`),
  getModel: apiCreator(dataSrc, 'GET', 'REQUEST', `${basePath}/experiments/:experimentId/models/:modelId`),
  pollModel: apiCreator(dataSrc, 'GET', 'POLL', `${basePath}/experiments/:experimentId/models/:modelId`),
  getSplitDetails: apiCreator(dataSrc, 'GET', 'REQUEST', `${basePath}/experiments/:experimentId/splits/:splitId`),
  getSplitStatus: apiCreator(dataSrc, 'GET', 'POLL', `${basePath}/experiments/:experimentId/splits/:splitId/status`),
  getAlgorithms: apiCreator(dataSrc, 'GET', 'REQUEST', `${basePath}/algorithms`),
  getModelStatus: apiCreator(dataSrc, 'GET', 'REQUEST', `${basePath}/experiments/:experimentId/models/:modelId/status`),
  pollModelStatus: apiCreator(dataSrc, 'GET', 'POLL', `${basePath}/experiments/:experimentId/models/:modelId/status`),
  trainModel: apiCreator(dataSrc, 'POST', 'REQUEST', `${basePath}/experiments/:experimentId/models/:modelId/train`),
  getSplitsInExperiment: apiCreator(dataSrc, 'GET', 'REQUEST', `${basePath}/experiments/:experimentId/splits`),

  deleteModelInExperiment: apiCreator(dataSrc, 'DELETE', 'REQUEST', `${basePath}/experiments/:experimentId/models/:modelId`),
  deleteExperiment: apiCreator(dataSrc, 'DELETE', 'REQUEST', `${basePath}/experiments/:experimentId`),

  createExperiment: apiCreator(dataSrc, 'PUT', 'REQUEST', `${basePath}/experiments/:experimentId`),
  createSplit: apiCreator(dataSrc, 'POST', 'REQUEST', `${basePath}/experiments/:experimentId/models/:modelId/split`),
  createModelInExperiment: apiCreator(dataSrc, 'POST', 'REQUEST', `${basePath}/experiments/:experimentId/models`),

  // Service management
  getApp: apiCreator(dataSrc, 'GET', 'REQUEST', `${appPath}`),
  startService: apiCreator(dataSrc, 'POST', 'REQUEST', `${servicePath}/start`),
  stopService: apiCreator(dataSrc, 'POST', 'REQUEST', `${servicePath}/stop`),
  createApp: apiCreator(dataSrc, 'PUT', 'REQUEST', `${appPath}`),
  pollServiceStatus: apiCreator(dataSrc, 'GET', 'POLL', `${servicePath}/status`),
  ping: apiCreator(dataSrc, 'GET', 'REQUEST', `${basePath}/experiments`)

};
