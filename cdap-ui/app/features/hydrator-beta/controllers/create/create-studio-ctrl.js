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

class HydratorCreateStudioControllerBeta {
  // Holy cow. Much DI. Such angular.
  constructor(LeftPanelStoreBeta, LeftPanelActionsFactoryBeta, ConfigActionsFactoryBeta, $stateParams, rConfig, ConfigStoreBeta, $rootScope, $scope, DetailNonRunsStore, NodeConfigStoreBeta, NodesActionsFactoryBeta, HydratorServiceBeta, ConsoleActionsFactory) {
    // This is required because before we fireup the actions related to the store, the store has to be initialized to register for any events.

    this.LeftPanelActionsFactoryBeta = LeftPanelActionsFactoryBeta;
    this.isExpanded = LeftPanelStoreBeta.getState();
    LeftPanelStoreBeta.registerOnChangeListener( () => {
      this.isExpanded = LeftPanelStoreBeta.getState();
    });
    // FIXME: This should essentially be moved to a scaffolding service that will do stuff for a state/view
    $scope.$on('$destroy', () => {
      DetailNonRunsStore.reset();
      NodeConfigStoreBeta.reset();
      ConsoleActionsFactory.resetMessages();
    });

    NodeConfigStoreBeta.init();
    if (rConfig) {
      ConfigActionsFactoryBeta.initializeConfigStore(rConfig);
      let configJson = rConfig;
      if (!rConfig.__ui__) {
        configJson = HydratorServiceBeta.getNodesAndConnectionsFromConfig(rConfig);
        configJson['__ui__'] = {
          nodes: configJson.nodes.map( (node) => {
            node.properties = node.plugin.properties;
            node.label = node.plugin.label;
            return node;
          })
        };
        configJson.config = {
          connections : configJson.connections
        };
      }

      NodesActionsFactoryBeta.createGraphFromConfig(configJson.__ui__.nodes, configJson.config.connections, configJson.config.comments);
    } else {
      ConfigActionsFactoryBeta.initializeConfigStore({});
    }
  }

  toggleSidebar() {
    this.LeftPanelActionsFactoryBeta.togglePanel();
  }
}

HydratorCreateStudioControllerBeta.$inject = ['LeftPanelStoreBeta', 'LeftPanelActionsFactoryBeta', 'ConfigActionsFactoryBeta', '$stateParams', 'rConfig', 'ConfigStoreBeta', '$rootScope', '$scope', 'DetailNonRunsStore', 'NodeConfigStoreBeta', 'NodesActionsFactoryBeta', 'HydratorServiceBeta', 'ConsoleActionsFactory'];
angular.module(PKG.name + '.feature.hydrator-beta')
  .controller('HydratorCreateStudioControllerBeta', HydratorCreateStudioControllerBeta);
