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
  .factory('myAdapterApi', function($resource, myHelpers, GLOBALS) {
    var templatePath = '/templates',
        adapterPath = '/namespaces/:namespace/apps/:adapter',

        listPath = '/namespaces/:namespace/apps?artifactName=' + GLOBALS.etlBatch + ',' + GLOBALS.etlRealtime,

        pluginFetchBase = '/namespaces/:namespace/artifacts/:adapterType/versions/:version/extensions/:extensionType',
        pluginsFetchPath = pluginFetchBase + '?scope=system',
        pluginDetailFetch = pluginFetchBase + '/plugins/:pluginName?scope=system';

    return $resource(
      '',
      {

      },
      {
        save: myHelpers.getConfig('PUT', 'REQUEST', adapterPath, false, {contentType: 'application/json'}),
        fetchTemplates: myHelpers.getConfig('GET', 'REQUEST', templatePath, true),
        fetchSources: myHelpers.getConfig('GET', 'REQUEST', pluginsFetchPath, true),
        fetchSinks: myHelpers.getConfig('GET', 'REQUEST', pluginsFetchPath, true),
        fetchTransforms: myHelpers.getConfig('GET', 'REQUEST', pluginsFetchPath, true),
        fetchSourceProperties: myHelpers.getConfig('GET', 'REQUEST', pluginDetailFetch, true),
        fetchSinkProperties: myHelpers.getConfig('GET', 'REQUEST', pluginDetailFetch, true),
        fetchTransformProperties: myHelpers.getConfig('GET', 'REQUEST', pluginDetailFetch, true),

        // FIXME: This needs to be replaced with fetching etl-batch & etl-realtime separately.
        list: myHelpers.getConfig('GET', 'REQUEST', listPath, true),
        pollStatus: myHelpers.getConfig('GET', 'POLL', adapterPath + '/status'),
        stopPollStatus: myHelpers.getConfig('GET', 'POLL-STOP', adapterPath + '/status'),
        delete: myHelpers.getConfig('DELETE', 'REQUEST', adapterPath),
        runs: myHelpers.getConfig('GET', 'REQUEST', adapterPath + '/runs', true),
        get: myHelpers.getConfig('GET', 'REQUEST', adapterPath),
        datasets: myHelpers.getConfig('GET', 'REQUEST', adapterPath + '/datasets', true),
        streams: myHelpers.getConfig('GET', 'REQUEST', adapterPath + '/streams', true),
        action: myHelpers.getConfig('POST', 'REQUEST', adapterPath + '/:action')
      }
    );
  });
