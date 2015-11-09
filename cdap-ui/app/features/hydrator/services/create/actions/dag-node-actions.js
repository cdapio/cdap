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

class PipelineCreateDAGActionsFactory{
  constructor(NodeConfigDispatcher) {
    this.nodeConfigDispatcher = NodeConfigDispatcher.getDispatcher();
  }
  addNode(node) {
    this.nodeConfigDispatcher.dispatch('onPluginAdd', node);
  }
  removeNode(node) {
    this.nodeConfigDispatcher.dispatch('onPluginRemove', node);
  }
  choosNode(node) {
    this.nodeConfigDispatcher.dispatch('onPluginChoose', node);
  }
}
PipelineCreateDAGActionsFactory.$inject = ['NodeConfigDispatcher'];
angular.module(`${PKG.name}.feature.hydrator`)
  .service('PipelineCreateDAGActionsFactory', PipelineCreateDAGActionsFactory);
