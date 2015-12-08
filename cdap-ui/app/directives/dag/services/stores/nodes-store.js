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

class NodesStore {
  constructor(NodesDispatcher, uuid) {
    this.state = {};
    this.setDefaults();
    this.changeListeners = [];
    this.uuid = uuid;

    let dispatcher = NodesDispatcher.getDispatcher();
    dispatcher.register('onNodeAdd', this.addNode.bind(this));
    dispatcher.register('onRemoveNode', this.removeNode.bind(this));
    dispatcher.register('onConnect', this.addConnection.bind(this));
    dispatcher.register('onConnectionsUpdate', this.updateConnections.bind(this));
    dispatcher.register('onRemoveConnection', this.removeConnection.bind(this));
    dispatcher.register('onReset', this.setDefaults.bind(this));
    dispatcher.register('onNodeSelect', this.setActiveNodeId.bind(this));
    dispatcher.register('onCreateGraphFromConfig', this.setNodesAndConnections.bind(this));
    dispatcher.register('onNodeSelectReset', this.resetActiveNode.bind(this));
    dispatcher.register('onNodeUpdate', this.updateNode.bind(this));
  }

  setDefaults() {
    this.state = {
      nodes: [],
      connections: [],
      activeNodeId: null
    };
  }

  reset() {
    this.changeListeners = [];
    this.setDefaults();
  }

  registerOnChangeListener(callback) {
    this.changeListeners.push(callback);
  }
  emitChange() {
    this.changeListeners.forEach( callback => callback() );
  }

  addNode(config) {
    if (!config.id) {
      config.id = (config.label || config.name) + '-' + this.uuid.v4();
    }
    this.state.nodes.push(config);
    this.emitChange();
  }
  updateNode(nodeId, config) {
    var matchNode = this.state.nodes.filter( node => node.id === nodeId);
    if (!matchNode.length) {
      return;
    }
    matchNode = matchNode[0];
    angular.extend(matchNode, config);
    this.emitChange();
  }
  removeNode(node) {
    let match = this.state.nodes.filter(n => n.id === node);
    this.state.nodes.splice(this.state.nodes.indexOf(match[0]), 1);
    this.state.activeNodeId = null;
    this.emitChange();
  }
  getNodes() {
    return this.state.nodes;
  }
  setNodes(nodes) {
    nodes.forEach(node => {
      if (!node.id) {
        node.id = node.name || (node.label + '-' + this.uuid.v4());
      }
    });
    this.state.nodes = nodes;
    this.emitChange();
  }
  getActiveNodeId() {
    return this.state.activeNodeId;
  }
  setActiveNodeId(nodeId) {
    this.state.activeNodeId = nodeId;
    this.emitChange();
  }
  resetActiveNode() {
    this.state.activeNodeId = null;
    this.emitChange();
  }

  addConnection(connection) {
    this.state.connections.push(connection);
    this.emitChange();
  }
  updateConnections(connections) {
    this.state.connections = connections;
    this.emitChange();
  }
  removeConnection(connection) {
    let index = this.state.connections.indexOf(connection);
    this.state.connections.splice(index, 1);
    this.emitChange();
  }
  getConnections() {
    return this.state.connections;
  }
  setConnections(connections) {
    this.state.connections = connections;
    this.emitChange();
  }

  setNodesAndConnections(nodes, connections) {
    this.setNodes(nodes);
    this.state.connections = connections;
    this.emitChange();
  }

}

NodesStore.$inject = ['NodesDispatcher', 'uuid'];
angular.module(`${PKG.name}.commons`)
  .service('NodesStore', NodesStore);
