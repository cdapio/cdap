/*
 * Copyright © 2017 Cask Data, Inc.
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
let basepath = '/namespaces/:namespace/apps/:appId/schedules';
let workflowPath = '/namespaces/:namespace/apps/:appId/workflows/:workflowId';

export const MyScheduleApi = {
  create: apiCreator(dataSrc, 'PUT', 'REQUEST', `${basepath}/:scheduleName`),
  delete: apiCreator(dataSrc, 'DELETE', 'REQUEST', `${basepath}/:scheduleName`),
  update: apiCreator(dataSrc, 'PUT', 'REQUEST', `${basepath}/:scheduleName/update`),
  get: apiCreator(dataSrc, 'GET', 'REQUEST', `${basepath}/:scheduleName`),
  getTriggers: apiCreator(dataSrc, 'GET', 'REQUEST', `${workflowPath}/schedules`),
  enableTrigger: apiCreator(dataSrc, 'POST', 'REQUEST', `${basepath}/:scheduleName/enable`),
  disableTrigger: apiCreator(dataSrc, 'POST', 'REQUEST', `${basepath}/:scheduleName/disable`),
  getTriggeredList: apiCreator(dataSrc, 'GET', 'REQUEST', '/namespaces/:namespace/schedules/trigger-type/program-status')
};
