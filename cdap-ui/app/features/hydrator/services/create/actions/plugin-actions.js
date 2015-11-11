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

class PluginActionsFactory {
  constructor(PluginsDispatcher, myPipelineApi, PluginConfigFactory, GLOBALS) {
    this.dispatcher = PluginsDispatcher.getDispatcher();
    this.api = myPipelineApi;
    this.pluginConfigApi = PluginConfigFactory;
    this.GLOBALS = GLOBALS;
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
}

PluginActionsFactory.$inject = ['PluginsDispatcher', 'myPipelineApi', 'PluginConfigFactory', 'GLOBALS'];
angular.module(`${PKG.name}.feature.hydrator`)
  .service('PluginActionsFactory', PluginActionsFactory);
