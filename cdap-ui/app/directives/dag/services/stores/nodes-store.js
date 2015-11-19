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
  constructor(NodesDispatcher) {
    this.state = {};
    this.setDefaults();
    this.changeListeners = [];

    let dispatcher = NodesDispatcher.getDispatcher();
    dispatcher.register('onNodeAdd', this.addNode.bind(this));
    dispatcher.register('onRemoveNode', this.removeNode.bind(this));
    dispatcher.register('onConnect', this.addConnection.bind(this));
    dispatcher.register('onConnectionsUpdate', this.updateConnections.bind(this));
    dispatcher.register('onRemoveConnection', this.removeConnection.bind(this));
    dispatcher.register('onReset', this.setDefaults.bind(this));
    dispatcher.register('onNodeSelect', this.setActiveNodeId.bind(this));
    dispatcher.register('onCreateGraphFromConfig', this.setNodesAndConnections.bind(this));
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
    this.state.nodes.push(config);
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
    this.state.nodes = nodes;
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
  getActiveNodeId() {
    return this.state.activeNodeId;
  }
  setConnections(connections) {
    this.state.connections = connections;
  }

  setNodesAndConnections(nodes, connections) {
    this.setNodes(nodes);
    this.setConnections(connections);
  }

  setActiveNodeId(nodeId) {
    this.state.activeNodeId = nodeId;
    this.emitChange();
  }
}

NodesStore.$inject = ['NodesDispatcher'];
angular.module(`${PKG.name}.commons`)
  .service('NodesStore', NodesStore);
