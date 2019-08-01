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

import DataSourceConfigurer from '../../services/datasource/DataSourceConfigurer';
import remoteDataSource from './remoteDataSource';
import {apiCreator} from '../../services/resource-helper';
import { USE_REMOTE_SERVER } from './config';

let dataSrc = DataSourceConfigurer.getInstance();


const appPath = '/namespaces/:namespace/apps/FeatureEngineeringApp';
const edaService = `${appPath}/services/AutoFeatureGenerationService/methods/eda`;

const EDADataServiceApi = {
  configurationConfig: serviceCreator(dataSrc, "GET", "REQUEST",`${edaService}/analysis/configparams/get`),
  availableSinks: serviceCreator(dataSrc, "GET", "REQUEST",`${edaService}/analysis/configparams/datasink/get`),
  createEDAPipeline: serviceCreator(dataSrc, "POST", "REQUEST",`${edaService}/:pipeline/analysis/create`),
  startEDAPipeline: serviceCreator(dataSrc, "POST", "REQUEST",`/namespaces/:namespace/apps/:pipeline/workflows/DataPipelineWorkflow/start`),
};

function serviceCreator (dataSrc, method, type, path, options = {}) {
  if (USE_REMOTE_SERVER) {
    dataSrc = remoteDataSource;
  }
  return apiCreator(dataSrc, method, type, path, options);
}

export default EDADataServiceApi;
