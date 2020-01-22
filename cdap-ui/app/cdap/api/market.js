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
import { apiCreatorAbsPath } from 'services/resource-helper';

let dataSrc = DataSourceConfigurer.getInstance();
const basepaths = window.CDAP_CONFIG.marketUrls;
// FIXME (CDAP-14836): Right now this is scattered across node and client. Need to consolidate this.
const REQUEST_TYPE_MARKET = 'MARKET';
const requestOptions = {
  requestOrigin: REQUEST_TYPE_MARKET,
};

function getVerifiedMarketHost(host) {
  return basepaths.find((element) => element === host);
}

export const MyMarketApi = {
  list: apiCreatorAbsPath(dataSrc, 'GET', 'REQUEST', `:marketHost/packages.json`, requestOptions),
  getMetaData: apiCreatorAbsPath(
    dataSrc,
    'GET',
    'REQUEST',
    `:marketHost/metadata.json`,
    requestOptions
  ),
  getCategories: apiCreatorAbsPath(
    dataSrc,
    'GET',
    'REQUEST',
    `:marketHost/categories.json`,
    requestOptions
  ),
  get: apiCreatorAbsPath(
    dataSrc,
    'GET',
    'REQUEST',
    `:marketHost/packages/:packageName/:version/spec.json`,
    requestOptions
  ),
  getCategoryIcon: (category, marketHost) => {
    const verifiedMarketHost = getVerifiedMarketHost(marketHost);
    return `${verifiedMarketHost}/categories/${category}/icon.png`;
  },
  getIcon: (entity, marketHost) => {
    const verifiedMarketHost = getVerifiedMarketHost(marketHost);
    return `${verifiedMarketHost}/packages/${entity.name}/${entity.version}/icon.png`;
  },
  getSampleData: apiCreatorAbsPath(
    dataSrc,
    'GET',
    'REQUEST',
    `:marketHost/packages/:entityName/:entityVersion/:filename`,
    requestOptions
  ),
};
