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

class HydratorPluPlusPostActionDetailCtrl {
  constructor(HydratorPlusPlusDetailNonRunsStore, $uibModal, myPipelineApi, $state, HydratorPlusPlusPluginConfigFactory) {
    this.HydratorPlusPlusDetailNonRunsStore = HydratorPlusPlusDetailNonRunsStore;
    this.$uibModal = $uibModal;
    this.myPipelineApi = myPipelineApi;
    this.$state = $state;
    this.HydratorPlusPlusPluginConfigFactory = HydratorPlusPlusPluginConfigFactory;

    this.postActions = [];
    this.config = {};
    this.setState();
  }
  setState() {
    let config = this.HydratorPlusPlusDetailNonRunsStore.getConfigJson();
    this.config = config;
    this.postActions = config.postActions || [];
  }

  viewAction(action) {
    let params = {
      namespace: this.$state.params.namespace,
      pipelineType: action.plugin.artifact.name,
      version: action.plugin.artifact.version,
      scope: action.plugin.artifact.scope,
      extensionType: action.plugin.type,
      pluginName: action.plugin.name
    };

    let plugin = angular.copy(action);

    this.myPipelineApi.fetchPostActionProperties(params)
      .$promise
      .then( (res) => {
        plugin._backendProperties = res[0].properties;
        this.fetchWidgets(plugin);
      });
  }

  // Fetching Widget JSON for the plugin
  fetchWidgets(action) {
    let artifact = {
      name: action.plugin.artifact.name,
      version: action.plugin.artifact.version,
      scope: action.plugin.artifact.scope,
      key: 'widgets.' + action.plugin.name + '-' + action.plugin.type
    };
    this.HydratorPlusPlusPluginConfigFactory
      .fetchWidgetJson(artifact.name, artifact.version, artifact.scope, artifact.key)
      .then( (widgetJson) => {
        let groupsConfig = this.HydratorPlusPlusPluginConfigFactory.generateNodeConfig(action._backendProperties, widgetJson);

        this.showModal(false, action, groupsConfig);

      }, () => {
        this.showModal(true, action);
      });
  }

  showModal(noConfig, action, groupsConfig) {
    this.$uibModal.open({
      keyboard: true,
      windowTopClass: 'hydrator-modal',
      templateUrl: '/assets/features/hydratorplusplus/templates/partial/postactions-modal.html',
      size: 'lg',
      controller: PostActionModalController,
      controllerAs: 'PostActionController',
      resolve: {
        modalProperties: () => {
          return {
            noConfig: noConfig,
            action: action,
            groupsConfig: groupsConfig
          };
        }
      }
    });
  }
}

function PostActionModalController (modalProperties) {
  'ngInject';

  this.pluginConfig = modalProperties.action;
  this.noConfig = modalProperties.noConfig;
  this.groupsConfig = modalProperties.groupsConfig;
}

HydratorPluPlusPostActionDetailCtrl.$inject = ['HydratorPlusPlusDetailNonRunsStore', '$uibModal', 'myPipelineApi', '$state', 'HydratorPlusPlusPluginConfigFactory'];
angular.module(`${PKG.name}.feature.hydratorplusplus`)
  .controller('HydratorPluPlusPostActionDetailCtrl', HydratorPluPlusPostActionDetailCtrl);
