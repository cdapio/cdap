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

class NodesActionsFactory {
  constructor(NodesDispatcher) {
    this.nodesDispatcher = NodesDispatcher.getDispatcher();
  }

  addNode(config) {
    this.nodesDispatcher.dispatch('onNodeAdd', config);
  }

  removeNode(node) {
    this.nodesDispatcher.dispatch('onRemoveNode', node);
  }

  addConnection(connection) {
    this.nodesDispatcher.dispatch('onConnect', connection);
  }

  removeConnection(connection) {
    this.nodesDispatcher.dispatch('onRemoveConnection', connection);
  }

  resetNodesAndConnections() {
    this.nodesDispatcher.dispatch('onReset');
  }

}
NodesActionsFactory.$inject = ['NodesDispatcher'];
angular.module(`${PKG.name}.commons`)
  .service('NodesActionsFactory', NodesActionsFactory);
