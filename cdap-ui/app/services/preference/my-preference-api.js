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
  .factory('myPreferenceApi', function(myCdapUrl, $resource, myAuth, myHelpers) {

    var url = myCdapUrl.constructUrl,
        basepath = '/namespaces/:namespace';

    return $resource(
      url({ _cdapPath: basepath }),
    {
      namespace: '@namespace',
      appId: '@appId',
      programType: '@programType',
      programId: '@programId',
    },
    {
      getSystemPreference: myHelpers.getConfig('GET', 'REQUEST', '/preferences'),
      setSystemPreference: myHelpers.getConfig('PUT', 'REQUEST', '/preferences'),
      getNamespacePreference: myHelpers.getConfig('GET', 'REQUEST', basepath + '/preferences'),
      setNamespacePreference: myHelpers.getConfig('PUT', 'REQUEST', basepath + '/preferences'),
      getAppPreference: myHelpers.getConfig('GET', 'REQUEST', basepath + '/apps/:appId/preferences'),
      setAppPreference: myHelpers.getConfig('PUT', 'REQUEST', basepath + '/apps/:appId/preferences'),
      getProgramPreference: myHelpers.getConfig('GET', 'REQUEST', basepath + '/apps/:appId/:programType/:programId/preferences'),
      setProgramPreference: myHelpers.getConfig('PUT', 'REQUEST', basepath + '/apps/:appId/:programType/:programId/preferences')
    });
  });
