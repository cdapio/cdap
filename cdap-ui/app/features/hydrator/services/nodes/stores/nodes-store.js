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
    dispatcher.register('onRemoveConnection', this.removeConnection.bind(this));

  }

  setDefaults() {
    this.state = {
      nodes: [],
      connections: []
    };
  }

  registerOnChangeListener(callback) {
    this.changeListeners.push(callback);
  }
  emitChange() {
    this.changeListeners.forEach( callback => callback() );
  }

  addNode(config) {
    // this.state.nodes[config.id] = config;
    this.state.nodes.push(config);
    this.emitChange();
  }
  removeNode(nodeId) {
    delete this.state.nodes[nodeId];
    this.emitChange();
  }
  getNodes() {
    return this.state.nodes;
  }

  addConnection(connection) {
    this.state.connections.push(connection);
    this.emitChange();
  }
  removeConnection(connection) {
    let index = this.state.connections.indexOf(connection);
    this.state.connections.splice(index, 1);
    this.emitChange();
  }
  getConnections() {
    return this.state.getConnections;
  }

}

NodesStore.$inject = ['NodesDispatcher'];
angular.module(`${PKG.name}.feature.hydrator`)
  .service('NodesStore', NodesStore);
