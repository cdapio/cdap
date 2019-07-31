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
const dataPrepSchemaService = `${appPath}/services/DataPrepSchemaService/methods/featureengineering`;
const fePipelineService = `${appPath}/services/FeatureEngineeringPipelineService/methods/featureengineering`;
const autoFeatureGenerationService = `${appPath}/services/AutoFeatureGenerationService/methods/featureengineering`;
const manualFeatureSelectionService = `${appPath}/services/ManualFeatureSelectionService/methods/featureengineering`;

const FEDataServiceApi = {
  pipelines: serviceCreator(dataSrc, "GET", "REQUEST",`${fePipelineService}/pipeline/getall?pipelineType=:type`),
  schema: serviceCreator(dataSrc, "GET", "REQUEST",`${dataPrepSchemaService}/dataschema/getall`),
  metadataConfig: serviceCreator(dataSrc, "GET", "REQUEST",`${autoFeatureGenerationService}/feature/generation/configparams/get?getSchemaParams=true`),
  engineConfig: serviceCreator(dataSrc, "GET", "REQUEST",`${autoFeatureGenerationService}/feature/generation/configparams/get?getSchemaParams=false`),
  createPipeline: serviceCreator(dataSrc, "POST", "REQUEST",`${autoFeatureGenerationService}/:pipeline/features/create`),
  readPipeline: serviceCreator(dataSrc, "GET", "REQUEST",`${autoFeatureGenerationService}/:pipeline/features/read`),
  updatePipeline: serviceCreator(dataSrc, "POST", "REQUEST",`${autoFeatureGenerationService}/:pipeline/features/edit`),
  deletePipeline: serviceCreator(dataSrc, "DELETE", "REQUEST",`${autoFeatureGenerationService}/:pipeline/features/delete`),
  pipelineData: serviceCreator(dataSrc, "GET", "REQUEST",`${manualFeatureSelectionService}/features/stats/get?pipelineName=:pipeline`),
  pipelineFilteredData: serviceCreator(dataSrc, "POST", "REQUEST",`${manualFeatureSelectionService}/:pipeline/features/filter`),
  readFSPipeline: serviceCreator(dataSrc, "GET", "REQUEST",`${manualFeatureSelectionService}/:pipeline/features/selected/read/pipeline`),
  featureCorrelationData: serviceCreator(dataSrc, "POST", "REQUEST",`${manualFeatureSelectionService}/:pipeline/features/correlation/:coefficientType/score/targetfeature/get`),
  saveFeaturePipeline: serviceCreator(dataSrc, "POST", "REQUEST",`${manualFeatureSelectionService}/:pipeline/features/selected/create/pipeline`),
  detectProperties:  serviceCreator(dataSrc, "POST", "REQUEST",`${dataPrepSchemaService}/dataset/selected/schema/identified/get`),
  availableSinks: serviceCreator(dataSrc, "GET", "REQUEST",`${autoFeatureGenerationService}/feature/generation/configparams/datasink/get`),
  modelBasedData: serviceCreator(dataSrc, "POST", "REQUEST",`${manualFeatureSelectionService}/:pipeline/features/modelbasedfeatureimportance/:coefficientType/score/targetfeature/get`),
};


function serviceCreator (dataSrc, method, type, path, options = {}) {
  if (USE_REMOTE_SERVER) {
    dataSrc = remoteDataSource;
  }
  return apiCreator(dataSrc, method, type, path, options);
}

export default FEDataServiceApi;
