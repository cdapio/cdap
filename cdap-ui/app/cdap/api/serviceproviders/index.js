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
import {apiCreator} from '../../services/resource-helper';

let dataSrc = DataSourceConfigurer.getInstance();
let basepath = '/system/serviceproviders';
let servicesbasepath = '/system/services';
let serviceidpath = `${servicesbasepath}/:serviceid`;

export const MyServiceProviderApi = {
  list: apiCreator(dataSrc, 'GET', 'REQUEST', basepath),
  pollList: apiCreator(dataSrc, 'GET', 'REQUEST', basepath),
  get: apiCreator(dataSrc, 'GET', 'REQUEST', `${basepath}/:serviceprovider/stats`),
  pollServicesList: apiCreator(dataSrc, 'GET', 'POLL', servicesbasepath),
  pollServiceStatus: apiCreator(dataSrc, 'GET', 'POLL', `${serviceidpath}/status`),
  setProvisions: apiCreator(dataSrc, 'PUT', 'REQUEST', `${serviceidpath}/instances`),
  getInstances: apiCreator(dataSrc, 'GET', 'REQUEST', `${serviceidpath}/instances`)
};
