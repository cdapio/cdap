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

class HydratorPlusPlusPostActionsCtrl {
  constructor(HydratorPlusPlusConfigStore, GLOBALS, myHelpers, myPipelineApi, $state, HydratorPlusPlusPluginConfigFactory, uuid, HydratorPlusPlusConfigActions, myAlertOnValium) {
    this.GLOBALS = GLOBALS;
    this.HydratorPlusPlusConfigStore = HydratorPlusPlusConfigStore;
    this.myHelpers = myHelpers;
    this.myPipelineApi = myPipelineApi;
    this.$state = $state;
    this.HydratorPlusPlusPluginConfigFactory = HydratorPlusPlusPluginConfigFactory;
    this.uuid = uuid;
    this.HydratorPlusPlusConfigActions = HydratorPlusPlusConfigActions;
    this.myAlertOnValium = myAlertOnValium;

    this.HydratorPlusPlusConfigStore.registerOnChangeListener(this.setState.bind(this));

    let artifact = this.HydratorPlusPlusConfigStore.getArtifact();
    this.postActionsList = [];

    let params = {
      namespace: this.$state.params.namespace,
      pipelineType: artifact.name,
      version: artifact.version,
      extensionType: 'postaction'
    };
    this.myPipelineApi.fetchPlugins(params)
      .$promise
      .then( (res) => {
        this.postActionsList = res;
      });

    this.setState();

  }
  setState() {
    this.postactions = this.HydratorPlusPlusConfigStore.getPostActions();
    this.activeTab = 'OVERVIEW';
  }

  tabClick(tab) {
    this.activeTab = tab;

    if (tab === 'OVERVIEW') { return; }

    this.pluginConfig = {
      name: tab.name + '-' + this.uuid.v4(),
      plugin: {
        name: tab.name,
        type: tab.type,
        artifact: tab.artifact,
        properties: {}
      }
    };

    let params = {
      namespace: this.$state.params.namespace,
      pipelineType: tab.artifact.name,
      version: tab.artifact.version,
      scope: tab.artifact.scope,
      extensionType: tab.type,
      pluginName: tab.name
    };

    this.myPipelineApi.fetchPluginProperties(params)
      .$promise
      .then( (res) => {
        this.pluginConfig._backendProperties = res[0].properties;

        let artifact = {
          name: tab.artifact.name,
          version: tab.artifact.version,
          scope: tab.artifact.scope,
          key: 'widgets.' + tab.name + '-' + tab.type
        };
        this.HydratorPlusPlusPluginConfigFactory
          .fetchWidgetJson(artifact.name, artifact.version, artifact.scope, artifact.key)
          .then( (widgetJson) => {
            this.noConfig = false;

            this.groupsConfig = this.HydratorPlusPlusPluginConfigFactory.generateNodeConfig(this.pluginConfig._backendProperties, widgetJson);

            // Initializing default value
            angular.forEach(this.groupsConfig.groups, (group) => {
              angular.forEach(group.fields, (field) => {
                if (field.defaultValue) {
                  this.pluginConfig.properties[field.name] = this.pluginConfig.properties[field.name] || field.defaultValue;
                }
              });
            });
          }, () => {
            this.noConfig = true;
          });
      });

  }

  cancel() {
    this.activeTab = 'OVERVIEW';
    this.pluginConfig = {};
  }

  add() {
    let isValid = true;
    angular.forEach(this.pluginConfig._backendProperties, (value, key) => {
      if (value.required && !this.pluginConfig.plugin.properties[key]) {
        isValid = false;
      }
    });

    if (!isValid) {
      this.myAlertOnValium.show({
        type: 'danger',
        content: 'Please fill out all required field'
      });
      return;
    }

    delete this.pluginConfig._backendProperties;
    this.HydratorPlusPlusConfigActions.addPostAction(angular.copy(this.pluginConfig));
    this.pluginConfig = {};
  }

  deleteAction(config) {
    this.HydratorPlusPlusConfigActions.deletePostAction(config);
  }
}

HydratorPlusPlusPostActionsCtrl.$inject = ['HydratorPlusPlusConfigStore', 'GLOBALS', 'myHelpers', 'myPipelineApi', '$state', 'HydratorPlusPlusPluginConfigFactory', 'uuid', 'HydratorPlusPlusConfigActions', 'myAlertOnValium'];
angular.module(`${PKG.name}.feature.hydratorplusplus`)
  .controller('HydratorPlusPlusPostActionsCtrl', HydratorPlusPlusPostActionsCtrl);
