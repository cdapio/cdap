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

import DataSourceConfigurer from 'services/datasource/DataSourceConfigurer';
import { apiCreator } from 'services/resource-helper';
let dataSrc = DataSourceConfigurer.getInstance();

var basepath = '/namespaces/:namespace';
var profilesPath = `${basepath}/profiles`;
var systemProfilesPath = `/profiles`;
var provisionersPath = '/provisioners';

export const MyCloudApi = {
  list: apiCreator(dataSrc, 'GET', 'REQUEST', profilesPath),
  getSystemProfiles: apiCreator(dataSrc, 'GET', 'REQUEST', `${systemProfilesPath}`),
  create: apiCreator(dataSrc, 'PUT', 'REQUEST', `${profilesPath}/:profile`),
  createSystemProfile: apiCreator(dataSrc, 'PUT', 'REQUEST', `${systemProfilesPath}/:profile`),
  get: apiCreator(dataSrc, 'GET', 'REQUEST', `${profilesPath}/:profile`),
  getSystemProfile: apiCreator(dataSrc, 'GET', 'REQUEST', `${systemProfilesPath}/:profile`),
  deleteSystemProfile: apiCreator(dataSrc, 'DELETE', 'REQUEST', `${systemProfilesPath}/:profile`),
  delete: apiCreator(dataSrc, 'DELETE', 'REQUEST', `${profilesPath}/:profile`),
  toggleSystemProfileStatus: apiCreator(
    dataSrc,
    'POST',
    'REQUEST',
    `${systemProfilesPath}/:profile/:action`
  ),
  toggleProfileStatus: apiCreator(dataSrc, 'POST', 'REQUEST', `${profilesPath}/:profile/:action`),

  getProvisioners: apiCreator(dataSrc, 'GET', 'REQUEST', `${provisionersPath}`),
  getProvisionerDetailSpec: apiCreator(
    dataSrc,
    'GET',
    'REQUEST',
    `${provisionersPath}/:provisioner`
  ),
};
