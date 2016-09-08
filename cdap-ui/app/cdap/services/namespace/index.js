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

import Datasource from '../datasource';

export default class MyNamespaceApi {
  constructor() {
    this.dataSrc = new Datasource();

    let basepath = '/namespaces';

    this.apiList = {
      list: this.createApis('GET', 'REQUEST', basepath),
      get: this.createApis('GET', 'REQUEST', basepath + '/:namespaceId')
    };
  }

  getApi() {
    return this.apiList;
  }

  createApis (method, type, path, options = {}) {
    return (params = {}, body) => {
      let url = path.split('/')
        .map((singlePath) => {
          if (singlePath[0] !== ':') {
            return singlePath;
          }

          let replacedPath = params[singlePath.slice(1)];
          if (!replacedPath) {
            console.warn(`${singlePath} cannot be found in params object`);
          }

          return replacedPath;
        })
        .join('/');

      let reqObj = Object.assign({ _cdapPath: url }, options);
      if (body) {
        reqObj = Object.assign({}, reqObj, { body });
      }

      if (type === 'REQUEST') {
        return this.dataSrc.request(reqObj);
      } else if (type === 'POLL') {
        return this.dataSrc.poll(reqObj);
      }
    };
  }
}
