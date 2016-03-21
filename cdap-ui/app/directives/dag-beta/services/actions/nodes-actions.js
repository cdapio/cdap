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

class DAGPlusPlusNodesActionsFactory {
  constructor(DAGPlusPlusNodesDispatcher, GLOBALS, DAGPlusPlusFactory, DAGPlusPlusNodesStore) {
    this.GLOBALS = GLOBALS;
    this.MyDAGFactory = DAGPlusPlusFactory;
    this.DAGPlusPlusNodesStore = DAGPlusPlusNodesStore;

    this.nodesDispatcher = DAGPlusPlusNodesDispatcher.getDispatcher();
  }

  addNode(config) {
    let canvasPanning = this.DAGPlusPlusNodesStore.getCanvasPanning();

    let sourcePosition = {
      top: 150 - canvasPanning.top,
      left: (10/100 * document.documentElement.clientWidth) - canvasPanning.left
    };
    let transformPosition = {
      top: 150 - canvasPanning.top,
      left: (30/100 * document.documentElement.clientWidth) - canvasPanning.left
    };
    let sinkPosition = {
      top: 150 - canvasPanning.top,
      left: (50/100 * document.documentElement.clientWidth) - canvasPanning.left
    };

    let offset = 35;


    // set initial position
    switch (this.GLOBALS.pluginConvert[config.type]) {
      case 'source':
        let sourceOffset = this.DAGPlusPlusNodesStore.getSourceCount() * offset;
        config._uiPosition = {
          top: (sourcePosition.top + sourceOffset) + 'px',
          left: (sourcePosition.left + sourceOffset) + 'px'
        };
        this.nodesDispatcher.dispatch('onAddSourceCount');
        break;
      case 'transform':
        let transformOffset = this.DAGPlusPlusNodesStore.getTransformCount() * offset;
        config._uiPosition = {
          top: (transformPosition.top + transformOffset) + 'px',
          left: (transformPosition.left + transformOffset) + 'px'
        };
        this.nodesDispatcher.dispatch('onAddTransformCount');
        break;
      case 'sink':
        let sinkOffset = this.DAGPlusPlusNodesStore.getSinkCount() * offset;
        config._uiPosition = {
          top: (sinkPosition.top + sinkOffset) + 'px',
          left: (sinkPosition.left + sinkOffset) + 'px'
        };
        this.nodesDispatcher.dispatch('onAddSinkCount');
        break;
    }

    this.nodesDispatcher.dispatch('onNodeAdd', config);
  }
  updateNode(nodeId, config) {
    this.nodesDispatcher.dispatch('onNodeUpdate', nodeId, config);
  }

  removeNode(node) {
    this.nodesDispatcher.dispatch('onRemoveNode', node);
  }

  setNodes(nodes) {
    this.nodesDispatcher.dispatch('onSetNodes', nodes);
  }

  addConnection(connection) {
    this.nodesDispatcher.dispatch('onConnect', connection);
  }
  setConnections(connections) {
    this.nodesDispatcher.dispatch('onConnectionsUpdate', connections);
  }
  removeConnection(connection) {
    this.nodesDispatcher.dispatch('onRemoveConnection', connection);
  }

  resetNodesAndConnections() {
    this.nodesDispatcher.dispatch('onReset');
  }

  selectNode(nodeName) {
    this.nodesDispatcher.dispatch('onNodeSelect', nodeName);
  }
  resetSelectedNode() {
    this.nodesDispatcher.dispatch('onNodeSelectReset');
  }

  resetPluginCount() {
    this.nodesDispatcher.dispatch('onResetPluginCount');
  }

  setCanvasPanning(panning) {
    this.nodesDispatcher.dispatch('onSetCanvasPanning', panning);
  }

  createGraphFromConfig(nodes, connections, comments) {
    this.DAGPlusPlusNodesStore.setDefaults();
    this.nodesDispatcher.dispatch('onCreateGraphFromConfig', nodes, connections, comments);
  }

  addComment(comment) {
    this.nodesDispatcher.dispatch('onAddComment', comment);
  }
  setComments(comments) {
    this.nodesDispatcher.dispatch('onSetComments', comments);
  }
  deleteComment(comment) {
    this.nodesDispatcher.dispatch('onDeleteComment', comment);
  }
  updateComment(commentId, config) {
    this.nodesDispatcher.dispatch('onUpdateComment', commentId, config);
  }

}
DAGPlusPlusNodesActionsFactory.$inject = ['DAGPlusPlusNodesDispatcher', 'GLOBALS', 'DAGPlusPlusFactory', 'DAGPlusPlusNodesStore'];
angular.module(`${PKG.name}.commons`)
  .service('DAGPlusPlusNodesActionsFactory', DAGPlusPlusNodesActionsFactory);
