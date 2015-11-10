/*
 * Copyright © 2015 Cask Data, Inc.
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

angular.module(PKG.name + '.services')
  .factory('myStreamApi', function(myCdapUrl, $resource, myAuth, myHelpers) {

    var url = myCdapUrl.constructUrl,
        listPath = '/namespaces/:namespace/streams',
        basepath = '/namespaces/:namespace/streams/:streamId';

    return $resource(
      url({ _cdapPath: basepath }),
    {
      namespace: '@namespace',
      streamId: '@streamId'
    },
    {
      list: myHelpers.getConfig('GET', 'REQUEST', listPath, true),
      get: myHelpers.getConfig('GET', 'REQUEST', basepath),
      create: myHelpers.getConfig('PUT', 'REQUEST', basepath),
      setProperties: myHelpers.getConfig('PUT', 'REQUEST', basepath + '/properties'),
      truncate: myHelpers.getConfig('POST', 'REQUEST', basepath + '/truncate'),
      programsList: myHelpers.getConfig('GET', 'REQUEST', basepath + '/programs', true),
      eventSearch: myHelpers.getConfig('GET', 'REQUEST', basepath + '/events', true),
      sendEvent: myHelpers.getConfig('POST', 'REQUEST', basepath, false, {json: false}),
      delete: myHelpers.getConfig('DELETE', 'REQUEST', basepath)
    });
  });
