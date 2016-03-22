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

class HydratorPlusPlusPluginActions {
  constructor(HydratorPlusPlusPluginsDispatcher, myPipelineApi, HydratorPlusPlusPluginConfigFactory, GLOBALS, mySettings) {
    this.dispatcher = HydratorPlusPlusPluginsDispatcher.getDispatcher();
    this.api = myPipelineApi;
    this.pluginConfigApi = HydratorPlusPlusPluginConfigFactory;
    this.GLOBALS = GLOBALS;
    this.mySettings = mySettings;
  }
  fetchArtifacts(params) {
    this.api
        .fetchArtifacts(params)
        .$promise
        .then(
          res => this.dispatcher.dispatch('onArtifactsFetch', res),
          err => console.log('ERR: Fetching list of artifacts failed', err)
        );
  }
  fetchSources(params) {
    let type = this.GLOBALS.pluginTypes[params.pipelineType]['source'];
    params.extensionType = type;
    this.api
        .fetchSources(params)
        .$promise
        .then(
          res => this.dispatcher.dispatch('onSourcesFetch', res, type),
          err => this.dispatcher.dispatch('onSourcesFetch', {err: err})
        );
  }
  fetchSinks(params) {
    let type = this.GLOBALS.pluginTypes[params.pipelineType]['sink'];
    params.extensionType = type;
    this.api
        .fetchSinks(params)
        .$promise
        .then(
          res => this.dispatcher.dispatch('onSinksFetch', res, type),
          err => this.dispatcher.dispatch('onSinksFetch', {err: err})
        );
  }
  fetchTransforms(params) {
    let type = this.GLOBALS.pluginTypes[params.pipelineType]['transform'];
    params.extensionType = type;
    this.api
        .fetchTransforms(params)
        .$promise
        .then(
          res => this.dispatcher.dispatch('onTransformsFetch', res, type),
          err => this.dispatcher.dispatch('onTransformsFetch', {err: err})
        );
  }
  fetchProperties(params) {
    this.api
        .fetchPluginProperties(params)
        .$promise
        .then(
          res => this.dispatcher.dispatch('onPluginPropertiesFetch', {properties: res, params: params}),
          err => this.dispatcher.dispatch('onPluginPropertiesFetch', {err: err, params: params})
        );
  }
  fetchNodeConfig(params) {
    let requiredParams = {
      templateid: params.templateid,
      pluginid: params.pluginid
    };
    this.pluginConfigApi
        .fetch(params.scope, params.templateid, params.pluginid)
        .then(
          res => this.dispatcher.dispatch('onPluginConfigFetch', {config: res, params: requiredParams}),
          err => this.dispatcher.dispatch('onPluginConfigFetch', {err: err, params: requiredParams})
        );
  }
  fetchTemplates(params) {
    this.mySettings
        .get('pluginTemplates')
        .then(res => {
          this.dispatcher.dispatch('onPluginTemplatesFetch', res, params);
        });

  }
}

HydratorPlusPlusPluginActions.$inject = ['HydratorPlusPlusPluginsDispatcher', 'myPipelineApi', 'HydratorPlusPlusPluginConfigFactory', 'GLOBALS', 'mySettings'];
angular.module(`${PKG.name}.feature.hydratorplusplus`)
  .service('HydratorPlusPlusPluginActions', HydratorPlusPlusPluginActions);
