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

class WizardConfigureConfirmStepCtrl {
  constructor($state, myPipelineApi, HydratorPlusPlusPluginConfigFactory) {
    this.$state = $state;
    this.myPipelineApi = myPipelineApi;
    this.HydratorPlusPlusPluginConfigFactory = HydratorPlusPlusPluginConfigFactory;
    this.showLoadingIcon = true;
    this.action.properties = this.action.properties || {};
    this.pluginFetch(this.action)
      .then( () => this.showLoadingIcon = false);
  }
  // Fetching Backend Properties
  pluginFetch(action) {
    this.errorInConfig = false;
    let params = {
      namespace: this.$state.params.namespace,
      pipelineType: action.artifact.name,
      version: action.artifact.version,
      scope: action.artifact.scope,
      extensionType: action.type,
      pluginName: action.name
    };

    return this.myPipelineApi.fetchPostActionProperties(params)
      .$promise
      .then( (res) => {
        this.action._backendProperties = res[0].properties;
        this.fetchWidgets(action);
      });
  }

  // Fetching Widget JSON for the plugin
  fetchWidgets(action) {
    let artifact = {
      name: action.artifact.name,
      version: action.artifact.version,
      scope: action.artifact.scope,
      key: 'widgets.' + action.name + '-' + action.type
    };
    return this.HydratorPlusPlusPluginConfigFactory
      .fetchWidgetJson(artifact.name, artifact.version, artifact.scope, artifact.key)
      .then( (widgetJson) => {
        this.noConfig = false;

        this.groupsConfig = this.HydratorPlusPlusPluginConfigFactory.generateNodeConfig(this.action._backendProperties, widgetJson);

        // Initializing default value
        angular.forEach(this.groupsConfig.groups, (group) => {
          angular.forEach(group.fields, (field) => {
            if (field.defaultValue) {
              this.action.properties[field.name] = this.action.properties[field.name] || field.defaultValue;
            }
          });
        });
      }, () => {
        this.noConfig = true;
      });
  }
}

WizardConfigureConfirmStepCtrl.$inject = ['$state', 'myPipelineApi', 'HydratorPlusPlusPluginConfigFactory'];

angular.module(PKG.name + '.commons')
  .controller('WizardConfigureConfirmStepCtrl', WizardConfigureConfirmStepCtrl);
