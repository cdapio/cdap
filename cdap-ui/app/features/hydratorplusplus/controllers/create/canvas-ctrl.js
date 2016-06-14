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
            rPlugin: function() {
              let appType = this.HydratorPlusPlusConfigStore.getAppType();
              return {
                node: pluginNode,
                isValidPlugin: true,
                type: appType,
                isSource: this.GLOBALS.pluginTypes[appType].source === pluginNode.type,
                isSink: this.GLOBALS.pluginTypes[appType].sink === pluginNode.type,
                isTransform: this.GLOBALS.pluginTypes[appType].transform === pluginNode.type
              };
            }.bind(this)
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
