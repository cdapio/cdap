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
  .factory('myPipelineApi', function($resource, myHelpers, GLOBALS) {
    var templatePath = '/templates',
        pipelinePath = '/namespaces/:namespace/apps/:pipeline',

        listPath = '/namespaces/:namespace/apps?artifactName=' + GLOBALS.etlBatch + ',' + GLOBALS.etlRealtime + ',' + GLOBALS.etlDataPipeline,
        artifactsPath = '/namespaces/:namespace/artifacts?scope=SYSTEM',
        extensionsFetchBase = '/namespaces/:namespace/artifacts/:pipelineType/versions/:version/extensions',
        pluginFetchBase = extensionsFetchBase + '/:extensionType',
        pluginsFetchPath = pluginFetchBase + '?scope=system',
        extensionsFetchPath = extensionsFetchBase + '?scope=system',
        pluginDetailFetch = pluginFetchBase + '/plugins/:pluginName?scope=system',
        artifactPropertiesPath = '/namespaces/:namespace/artifacts/:artifactName/versions/:artifactVersion/properties';


    return $resource(
      '',
      {

      },
      {
        save: myHelpers.getConfig('PUT', 'REQUEST', pipelinePath, false, {contentType: 'application/json'}),
        fetchArtifacts: myHelpers.getConfig('GET', 'REQUEST', artifactsPath, true),
        fetchExtensions: myHelpers.getConfig('GET', 'REQUEST', extensionsFetchPath, true),
        fetchSources: myHelpers.getConfig('GET', 'REQUEST', pluginsFetchPath, true),
        fetchSinks: myHelpers.getConfig('GET', 'REQUEST', pluginsFetchPath, true),
        fetchTransforms: myHelpers.getConfig('GET', 'REQUEST', pluginsFetchPath, true),
        fetchTemplates: myHelpers.getConfig('GET', 'REQUEST', templatePath, true),
        fetchPlugins: myHelpers.getConfig('GET', 'REQUEST', pluginsFetchPath, true),
        fetchSourceProperties: myHelpers.getConfig('GET', 'REQUEST', pluginDetailFetch, true),
        fetchSinkProperties: myHelpers.getConfig('GET', 'REQUEST', pluginDetailFetch, true),
        fetchTransformProperties: myHelpers.getConfig('GET', 'REQUEST', pluginDetailFetch, true),

        fetchArtifactProperties: myHelpers.getConfig('GET', 'REQUEST', artifactPropertiesPath),

        // The above three could be replaced by this one.
        fetchPluginProperties: myHelpers.getConfig('GET', 'REQUEST', pluginDetailFetch, true),

        // FIXME: This needs to be replaced with fetching etl-batch & etl-realtime separately.
        list: myHelpers.getConfig('GET', 'REQUEST', listPath, true),
        pollStatus: myHelpers.getConfig('GET', 'POLL', pipelinePath + '/status'),
        stopPollStatus: myHelpers.getConfig('GET', 'POLL-STOP', pipelinePath + '/status'),
        delete: myHelpers.getConfig('DELETE', 'REQUEST', pipelinePath),
        runs: myHelpers.getConfig('GET', 'REQUEST', pipelinePath + '/runs', true),
        get: myHelpers.getConfig('GET', 'REQUEST', pipelinePath),
        datasets: myHelpers.getConfig('GET', 'REQUEST', pipelinePath + '/datasets', true),
        streams: myHelpers.getConfig('GET', 'REQUEST', pipelinePath + '/streams', true),
        action: myHelpers.getConfig('POST', 'REQUEST', pipelinePath + '/:action')
      }
    );
  });
