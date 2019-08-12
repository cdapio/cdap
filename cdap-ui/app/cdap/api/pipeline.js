/*
 * Copyright © 2016-2018 Cask Data, Inc.
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

const dataSrc = DataSourceConfigurer.getInstance();
const basepath = '/namespaces/:namespace/apps/:appId';
const artifactBasePath = `/namespaces/:namespace/artifacts/:artifactName/versions/:artifactVersion/properties`;
const statsPath = `${basepath}/workflows/:workflowId/statistics?start=0`;
const schedulePath = `${basepath}/schedules/:scheduleId`;
const programPath = `${basepath}/:programType/:programName`;
const runsCountPath = '/namespaces/:namespace/runcount';
const batchRunsPath = '/namespaces/:namespace/runs';
const pluginsPath =
  '/namespaces/:namespace/artifacts/:parentArtifact/versions/:version/extensions/:extension/plugins/:pluginName';

export const MyPipelineApi = {
  list: apiCreator(dataSrc, 'GET', 'REQUEST', '/namespaces/:namespace/apps'),
  publish: apiCreator(dataSrc, 'PUT', 'REQUEST', basepath),

  schedule: apiCreator(dataSrc, 'POST', 'REQUEST', `${schedulePath}/resume`),
  suspend: apiCreator(dataSrc, 'POST', 'REQUEST', `${schedulePath}/suspend`),
  getScheduleStatus: apiCreator(dataSrc, 'GET', 'REQUEST', `${schedulePath}/status`),

  getStatistics: apiCreator(dataSrc, 'GET', 'REQUEST', statsPath),
  getRunDetails: apiCreator(dataSrc, 'GET', 'REQUEST', `${programPath}/runs/:runid`),
  getRuns: apiCreator(dataSrc, 'GET', 'REQUEST', `${programPath}/runs`),
  pollRuns: apiCreator(dataSrc, 'GET', 'POLL', `${programPath}/runs`),
  getRunsCount: apiCreator(dataSrc, 'POST', 'REQUEST', `${runsCountPath}`),
  pollRunsCount: apiCreator(dataSrc, 'POST', 'POLL', `${runsCountPath}`),
  getNextRunTime: apiCreator(dataSrc, 'GET', 'REQUEST', `${programPath}/nextruntime)`),
  fetchMacros: apiCreator(dataSrc, 'GET', 'REQUEST', `${basepath}/plugins`),
  fetchWidgetJson: apiCreator(dataSrc, 'GET', 'REQUEST', artifactBasePath),
  get: apiCreator(dataSrc, 'GET', 'REQUEST', basepath),
  pollStatistics: apiCreator(dataSrc, 'GET', 'REQUEST', statsPath),
  getNextRun: apiCreator(
    dataSrc,
    'GET',
    'REQUEST',
    `${basepath}/:programType/:programName/nextruntime`
  ),
  getBatchRuns: apiCreator(dataSrc, 'POST', 'REQUEST', batchRunsPath),
  delete: apiCreator(dataSrc, 'DELETE', 'REQUEST', basepath),

  getPluginProperties: apiCreator(dataSrc, 'GET', 'REQUEST', pluginsPath),
};
