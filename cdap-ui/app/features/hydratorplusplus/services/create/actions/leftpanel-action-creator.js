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

class HydratorPlusPlusPluginActions {
  constructor(myPipelineApi, GLOBALS, mySettings, LEFTPANELSTORE_ACTIONS) {
    this.api = myPipelineApi;
    this.GLOBALS = GLOBALS;
    this.mySettings = mySettings;
    this.leftpanelactions = LEFTPANELSTORE_ACTIONS;
  }

  fetchExtensions(params) {
    return (dispatch) => {
      this.api
          .fetchExtensions(params)
          .$promise
          .then(
            res => dispatch({
              type: this.leftpanelactions.EXTENSIONS_FETCH,
              payload: Object.assign({}, { pipelineType: params.pipelineType, extensions: res })
            }),
            err => console.log('ERR: Fetching list of artifacts failed', err)
          );
    };
  }
  fetchPlugins(extension, params) {
    return (dispatch) => {
      return this.api
        .fetchPlugins(params)
        .$promise
        .then(
          res => dispatch({
            type: this.leftpanelactions.PLUGINS_FETCH,
            payload: { extension, plugins: res }
          }),
          err => console.log(`ERR: Fetch list of plugins for ${extension} failed: ${err}`)
        );
    };
  }
  fetchTemplates(params, pipelineObj) {
    const {pipelineType, namespace} = pipelineObj;
    return (dispatch) => {
      this.mySettings
          .get('pluginTemplates')
          .then(res => {
            if (!res) { return; }
            dispatch({
              type: this.leftpanelactions.PLUGIN_TEMPLATE_FETCH,
              payload: { res, pipelineType, namespace }
            });
          });
    };
  }
  fetchDefaultVersion() {
    return (dispatch) => {
      this.mySettings
          .get('plugin-default-version')
          .then( res => {
            if (!res) { return; }
            dispatch({
              type: this.leftpanelactions.PLUGINS_DEFAULT_VERSION_FETCH,
              payload: {res}
            });
          });
    };
  }
  updateDefaultVersion(plugin) {
    return (dispatch) => {
      this.mySettings
          .get('plugin-default-version')
          .then( res => {
            let key = `${plugin.name}-${plugin.type}-${plugin.artifact.name}`;
            res = res || {};
            res[key] = plugin.artifact;
            return this.mySettings.set('plugin-default-version', res);
          })
          .then(() => this.mySettings.get('plugin-default-version'))
          .then(res => {
            if (!res) { return; }
            dispatch({
              type: this.leftpanelactions.PLUGINS_DEFAULT_VERSION_FETCH,
              payload: {res}
            });
          });
    };
  }
}

HydratorPlusPlusPluginActions.$inject = ['myPipelineApi', 'GLOBALS', 'mySettings', 'LEFTPANELSTORE_ACTIONS'];
angular.module(`${PKG.name}.feature.hydratorplusplus`)
  .service('HydratorPlusPlusPluginActions', HydratorPlusPlusPluginActions);
