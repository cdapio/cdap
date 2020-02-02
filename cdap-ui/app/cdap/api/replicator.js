/*
 * Copyright © 2020 Cask Data, Inc.
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

const pluginPath = '/namespaces/:namespace/artifacts/delta-app/versions/0.1.0-SNAPSHOT/extensions';
const appPath = '/namespaces/:namespace/apps/:appName';
const programPath = `${appPath}/workers/DeltaWorker`;
const servicePath = `/namespaces/system/apps/delta/services/assessor/methods/v1/contexts/:namespace`;
const draftPath = `${servicePath}/drafts/:draftId`;

export const MyReplicatorApi = {
  getPlugins: apiCreator(dataSrc, 'GET', 'REQUEST', `${pluginPath}/:pluginType?scope=system`),
  publish: apiCreator(dataSrc, 'PUT', 'REQUEST', appPath),
  list: apiCreator(dataSrc, 'GET', 'REQUEST', '/namespaces/:namespace/apps?artifactName=delta-app'),
  pollStatus: apiCreator(dataSrc, 'GET', 'POLL', `${programPath}/status`),
  action: apiCreator(dataSrc, 'POST', 'REQUEST', `${programPath}/:action`),
  delete: apiCreator(dataSrc, 'DELETE', 'REQUEST', appPath),
  listDrafts: apiCreator(dataSrc, 'GET', 'REQUEST', `${servicePath}/drafts`),
  putDraft: apiCreator(dataSrc, 'PUT', 'REQUEST', draftPath),
  deleteDraft: apiCreator(dataSrc, 'DELETE', 'REQUEST', draftPath),
  getDraft: apiCreator(dataSrc, 'GET', 'REQUEST', draftPath),
  getTables: apiCreator(dataSrc, 'POST', 'REQUEST', `${draftPath}/listTables`),
  getTableInfo: apiCreator(
    dataSrc,
    'POST',
    'REQUEST',
    `${draftPath}/databases/:database/tables/:tableId/describe`
  ),
};
