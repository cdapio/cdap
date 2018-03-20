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
import {apiCreator} from 'services/resource-helper';

let dataSrc = DataSourceConfigurer.getInstance();
let basepath = '/namespaces/:namespace/apps/:appId';
let artifactBasePath = `/namespaces/:namespace/artifacts/:artifactName/versions/:artifactVersion/properties`;
let statsPath = `${basepath}/workflows/:workflowId/statistics?start=0`;
let schedulePath = `${basepath}/schedules/:scheduleId`;
let programPath = `${basepath}/:programType/:programName`;

export const MyPipelineApi = {
  publish: apiCreator(dataSrc, 'PUT', 'REQUEST', basepath),

  schedule: apiCreator(dataSrc, 'POST', 'REQUEST', `${schedulePath}/resume`),
  suspend: apiCreator(dataSrc, 'POST', 'REQUEST', `${schedulePath}/suspend`),
  getScheduleStatus: apiCreator(dataSrc, 'GET', 'REQUEST', `${schedulePath}/status`),

  getStatistics: apiCreator(dataSrc, 'GET', 'REQUEST', statsPath),
  getRuns: apiCreator(dataSrc, 'GET', 'REQUEST', `${programPath}/runs`),
  pollRuns: apiCreator(dataSrc, 'GET', 'POLL', `${programPath}/runs`),
  getNextRunTime: apiCreator(dataSrc, 'GET', 'REQUEST', `${programPath}/nextruntime)`),
  fetchMacros: apiCreator(dataSrc, 'GET', 'REQUEST', `${basepath}/plugins`),
  fetchWidgetJson: apiCreator(dataSrc, 'GET', 'REQUEST', artifactBasePath)
};
