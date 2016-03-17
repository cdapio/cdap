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

class HydratorCreateCanvasControllerBeta {
  constructor(BottomPanelStoreBeta, NodesStoreBeta, NodesActionsFactoryBeta, ConfigStoreBeta, PipelineNodeConfigActionFactoryBeta, HydratorServiceBeta) {
    this.NodesStoreBeta = NodesStoreBeta;
    this.ConfigStoreBeta = ConfigStoreBeta;
    this.PipelineNodeConfigActionFactoryBeta = PipelineNodeConfigActionFactoryBeta;
    this.NodesActionsFactoryBeta = NodesActionsFactoryBeta;
    this.HydratorServiceBeta = HydratorServiceBeta;

    this.setState = () => {
      this.state = {
        setScroll: (BottomPanelStoreBeta.getPanelState() === 0? false: true)
      };
    };
    this.setState();
    BottomPanelStoreBeta.registerOnChangeListener(this.setState.bind(this));

    this.nodes = [];
    this.connections = [];

    this.updateNodesAndConnections();
    NodesStoreBeta.registerOnChangeListener(this.updateNodesAndConnections.bind(this));
  }

  setStateAndUpdateConfigStore() {
    this.nodes = this.NodesStoreBeta.getNodes();
    this.connections = this.NodesStoreBeta.getConnections();
    this.ConfigStoreBeta.setNodes(this.nodes);
    this.ConfigStoreBeta.setConnections(this.connections);
    this.ConfigStoreBeta.setComments(this.NodesStoreBeta.getComments());
  }

  updateNodesAndConnections() {
    var activeNode = this.NodesStoreBeta.getActiveNodeId();
    if (!activeNode) {
      this.deleteNode();
    } else {
      this.setActiveNode();
    }
  }

  setActiveNode() {
    var nodeId = this.NodesStoreBeta.getActiveNodeId();
    if (!nodeId) {
      return;
    }
    var pluginNode;
    var nodeFromNodesStore;
    var nodeFromConfigStore = this.ConfigStoreBeta.getNodes().filter( node => node.name === nodeId );
    if (nodeFromConfigStore.length) {
      pluginNode = nodeFromConfigStore[0];
    } else {
      nodeFromNodesStore = this.NodesStoreBeta.getNodes().filter(node => node.name === nodeId);
      pluginNode = nodeFromNodesStore[0];
    }
    this.PipelineNodeConfigActionFactoryBeta.choosePlugin(pluginNode);
    this.setStateAndUpdateConfigStore();
  }

  deleteNode() {
    this.setStateAndUpdateConfigStore();
    this.PipelineNodeConfigActionFactoryBeta.removePlugin();
  }

  generateSchemaOnEdge(sourceId) {
    return this.HydratorServiceBeta.generateSchemaOnEdge(sourceId);
  }
}


HydratorCreateCanvasControllerBeta.$inject = ['BottomPanelStoreBeta', 'NodesStoreBeta', 'NodesActionsFactoryBeta', 'ConfigStoreBeta', 'PipelineNodeConfigActionFactoryBeta', 'HydratorServiceBeta'];
angular.module(PKG.name + '.feature.hydrator-beta')
  .controller('HydratorCreateCanvasControllerBeta', HydratorCreateCanvasControllerBeta);
