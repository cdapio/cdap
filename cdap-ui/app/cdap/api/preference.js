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

import DataSourceConfigurer from 'services/datasource/DataSourceConfigurer';
import { apiCreator } from 'services/resource-helper';

let dataSrc = DataSourceConfigurer.getInstance();
const basepath = '/namespaces/:namespace';

export const MyPreferenceApi = {
  getSystemPreferences: apiCreator(dataSrc, 'GET', 'REQUEST', '/preferences'),
  setSystemPreferences: apiCreator(dataSrc, 'PUT', 'REQUEST', '/preferences'),
  getNamespacePreferences: apiCreator(dataSrc, 'GET', 'REQUEST', `${basepath}/preferences`),
  setNamespacePreferences: apiCreator(dataSrc, 'PUT', 'REQUEST', `${basepath}/preferences`),
  getNamespacePreferencesResolved: apiCreator(
    dataSrc,
    'GET',
    'REQUEST',
    `${basepath}/preferences?resolved=true`
  ),
  getAppPreferences: apiCreator(dataSrc, 'GET', 'REQUEST', `${basepath}/apps/:appId/preferences`),
  setAppPreferences: apiCreator(dataSrc, 'PUT', 'REQUEST', `${basepath}/apps/:appId/preferences`),
  getAppPreferencesResolved: apiCreator(
    dataSrc,
    'GET',
    'REQUEST',
    `${basepath}/apps/:appId/preferences?resolved=true`
  ),
  getProgramPreferences: apiCreator(
    dataSrc,
    'GET',
    'REQUEST',
    `${basepath}/apps/:appId/:programType/:programId/preferences`
  ),
  setProgramPreferences: apiCreator(
    dataSrc,
    'PUT',
    'REQUEST',
    `${basepath}/apps/:appId/:programType/:programId/preferences`
  ),
};
