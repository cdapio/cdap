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

class HydratorPlusPlusCreateCanvasCtrl {
  constructor(HydratorPlusPlusBottomPanelStore, DAGPlusPlusNodesStore, HydratorPlusPlusConfigStore, HydratorPlusPlusNodeConfigActions, HydratorPlusPlusHydratorService, $uibModal, GLOBALS) {
    this.DAGPlusPlusNodesStore = DAGPlusPlusNodesStore;
    this.HydratorPlusPlusConfigStore = HydratorPlusPlusConfigStore;
    this.HydratorPlusPlusNodeConfigActions = HydratorPlusPlusNodeConfigActions;
    this.HydratorPlusPlusHydratorService = HydratorPlusPlusHydratorService;
    this.GLOBALS = GLOBALS;

    this.setState = () => {
      this.state = {
        setScroll: (HydratorPlusPlusBottomPanelStore.getPanelState() === 0? false: true)
      };
    };
    this.setState();
    HydratorPlusPlusBottomPanelStore.registerOnChangeListener(this.setState.bind(this));

    this.nodes = [];
    this.connections = [];
    this.$uibModal = $uibModal;
    DAGPlusPlusNodesStore.registerOnChangeListener(this.updateNodesAndConnections.bind(this));
  }

  setStateAndUpdateConfigStore() {
    this.nodes = this.DAGPlusPlusNodesStore.getNodes();
    this.connections = this.DAGPlusPlusNodesStore.getConnections();
    this.HydratorPlusPlusConfigStore.setNodes(this.nodes);
    this.HydratorPlusPlusConfigStore.setConnections(this.connections);
    this.HydratorPlusPlusConfigStore.setComments(this.DAGPlusPlusNodesStore.getComments());
  }

  updateNodesAndConnections() {
    var activeNode = this.DAGPlusPlusNodesStore.getActiveNodeId();
    if (!activeNode) {
      this.deleteNode();
    } else {
      this.setActiveNode();
    }
  }

  setActiveNode() {
    var nodeId = this.DAGPlusPlusNodesStore.getActiveNodeId();
    if (!nodeId) {
      return;
    }
    var pluginNode;
    var nodeFromNodesStore;
    var nodeFromConfigStore = this.HydratorPlusPlusConfigStore.getNodes().filter( node => node.name === nodeId );
    if (nodeFromConfigStore.length) {
      pluginNode = nodeFromConfigStore[0];
    } else {
      nodeFromNodesStore = this.DAGPlusPlusNodesStore.getNodes().filter(node => node.name === nodeId);
      pluginNode = nodeFromNodesStore[0];
    }
    this.$uibModal
        .open({
          templateUrl: '/assets/features/hydratorplusplus/templates/partial/node-config.html',
          size: 'lg',
          backdrop: 'static',
          windowTopClass: 'node-config-modal hydrator-modal',
          controller: 'HydratorPlusPlusNodeConfigCtrl',
          controllerAs: 'HydratorPlusPlusNodeConfigCtrl',
          resolve: {
            rDisabled: function() {
              return false;
            },
            rPlugin: ['HydratorPlusPlusNodeService', 'HydratorPlusPlusConfigStore', 'GLOBALS', function(HydratorPlusPlusNodeService, HydratorPlusPlusConfigStore, GLOBALS) {
              let pluginId = pluginNode.name;
              let appType = HydratorPlusPlusConfigStore.getAppType();
              let sourceConn = HydratorPlusPlusConfigStore
                .getSourceNodes(pluginId)
                .filter( node => typeof node.outputSchema === 'string');
              return HydratorPlusPlusNodeService
                .getPluginInfo(pluginNode, appType, sourceConn)
                .then((nodeWithInfo) => (
                  {
                    node: nodeWithInfo,
                    isValidPlugin: true,
                    type: appType,
                    isSource: GLOBALS.pluginTypes[appType].source === nodeWithInfo.type,
                    isSink: GLOBALS.pluginTypes[appType].sink === nodeWithInfo.type,
                    isTransform: GLOBALS.pluginTypes[appType].transform === nodeWithInfo.type
                  }
                ));
            }]
          }
        })
        .result
        .then(this.deleteNode.bind(this));
  }

  deleteNode() {
    this.setStateAndUpdateConfigStore();
    this.HydratorPlusPlusNodeConfigActions.removePlugin();
  }

  generateSchemaOnEdge(sourceId) {
    return this.HydratorPlusPlusHydratorService.generateSchemaOnEdge(sourceId);
  }
}


HydratorPlusPlusCreateCanvasCtrl.$inject = ['HydratorPlusPlusBottomPanelStore', 'DAGPlusPlusNodesStore', 'HydratorPlusPlusConfigStore', 'HydratorPlusPlusNodeConfigActions', 'HydratorPlusPlusHydratorService', '$uibModal', 'GLOBALS'];
angular.module(PKG.name + '.feature.hydratorplusplus')
  .controller('HydratorPlusPlusCreateCanvasCtrl', HydratorPlusPlusCreateCanvasCtrl);
