/*
 * Copyright © 2016-2017 Cask Data, Inc.
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
const basepath = '/namespaces/:namespace/artifacts';
const baseArtifactPath = basepath + '/:artifactId/versions/:version';
const basePluginArtifactJSON = baseArtifactPath + '/properties';

export const MyArtifactApi = {
  get: apiCreator(dataSrc, 'GET', 'REQUEST', baseArtifactPath ),
  listExtensions: apiCreator(dataSrc, 'GET', 'REQUEST', `${baseArtifactPath}/extensions`),
  listMicroservicePlugins: apiCreator(dataSrc, 'GET', 'REQUEST', `${baseArtifactPath}/extensions/microservice`),
  gettMicroservicePluginDetails: apiCreator(dataSrc, 'GET', 'REQUEST', `${baseArtifactPath}/extensions/microservice/plugins/:pluginId`),
  delete: apiCreator(dataSrc, 'DELETE', 'REQUEST', baseArtifactPath),
  loadPluginConfiguration: apiCreator(dataSrc, 'PUT', 'REQUEST', basePluginArtifactJSON),
  list: apiCreator(dataSrc, 'GET', 'REQUEST', basepath),
  reloadSystemArtifacts: apiCreator(dataSrc, 'POST', 'REQUEST', '/namespaces/system/artifacts')
};
