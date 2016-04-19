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
  constructor(HydratorPlusPlusDetailNonRunsStore, $uibModal, myPipelineApi, $state) {
    this.HydratorPlusPlusDetailNonRunsStore = HydratorPlusPlusDetailNonRunsStore;
    this.$uibModal = $uibModal;
    this.myPipelineApi = myPipelineApi;
    this.$state = $state;

    this.postactions = [];
    this.config = {};
    this.setState();
  }
  setState() {
    let config = this.HydratorPlusPlusDetailNonRunsStore.getConfigJson();
    this.config = config;
    console.log('config', config);
    this.postactions = config.postactions || [];
    console.log('actions', this.postactions);
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

    this.myPipelineApi.fetchPluginProperties(params)
      .$promise
      .then( (res) => {
        plugin._backendProperties = res[0].properties;
        // this.fetchWidgets(tab);
        this.$uibModal.open({
          keyboard: false,
          windowTopClass: 'hydrator-modal',
          templateUrl: '/assets/features/hydratorplusplus/templates/partial/postactions-modal.html',
          size: 'lg',
          controller: PostActionModalController,
          controllerAs: 'PostActionController',
          resolve: {
            pluginConfig: () => {
              return plugin;
            }
          }
        });

      });




  }
}

function PostActionModalController (pluginConfig) {
  'ngInject';

  this.pluginConfig = pluginConfig;
}

HydratorPluPlusPostActionDetailCtrl.$inject = ['HydratorPlusPlusDetailNonRunsStore', '$uibModal', 'myPipelineApi', '$state'];
angular.module(`${PKG.name}.feature.hydratorplusplus`)
  .controller('HydratorPluPlusPostActionDetailCtrl', HydratorPluPlusPostActionDetailCtrl);
