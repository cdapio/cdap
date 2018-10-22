/*
 * Copyright © 2016 Cask Data, Inc.
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
const basepath = '/namespaces/:namespace/data/explore/tables';
const queriesPath = '/namespaces/:namespace/data/explore/queries';
const queryHandleApi = '/data/explore/queries/:queryHandle';
const myExploreApi = {
  fetchTables: apiCreator(dataSrc, 'GET', 'REQUEST', basepath),
  fetchQueries: apiCreator(dataSrc, 'GET', 'POLL', queriesPath),
  submitQuery: apiCreator(dataSrc, 'POST', 'REQUEST', queriesPath),
  getQuerySchema: apiCreator(dataSrc, 'GET', 'REQUEST', queryHandleApi + '/schema'),
  getQueryPreview: apiCreator(dataSrc, 'POST', 'REQUEST', queryHandleApi + '/preview'),
  pollQueryStatus: apiCreator(dataSrc, 'GET', 'POLL', queryHandleApi + '/status'),
  download: apiCreator(dataSrc, 'POST', 'REQUEST', queryHandleApi + '/download'),
};

export default myExploreApi;
