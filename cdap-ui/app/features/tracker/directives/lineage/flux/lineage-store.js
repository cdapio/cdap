/*
 * Copyright © 2016 Cask Data, Inc.
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

class LineageStore {
  constructor($state, myLineageService) {
    this.myLineageService = myLineageService;

    this.state = {};
    this.setDefaults();
  }
  setDefaults() {
    this.state = {
      nodes: [],
      uniqueNodes: {},
      connections: [],
      graph: {}
    };

    this.changeListeners = [];
  }

  registerOnChangeListener(callback) {
    this.changeListeners.push(callback);
  }
  emitChange() {
    this.changeListeners.forEach( callback => callback() );
  }

  loadLineageData(data, params, method) {
    let obj;
    if (method === 'duplicate') {
      obj = this.myLineageService.parseLineageResponse(data, params);
    } else {
      obj = this.myLineageService.secondLineageParser(data, params);
    }

    this.state.nodes = obj.nodes;
    this.state.uniqueNodes = obj.uniqueNodes;
    this.state.connections = obj.connections;
    this.state.graph = obj.graph;

    this.emitChange();
  }
  getNodes() {
    return this.state.nodes;
  }
  getUniqueNodes() {
    return this.state.uniqueNodes;
  }
  getConnections() {
    return this.state.connections;
  }
  getGraph() {
    return this.state.graph;
  }


}

LineageStore.$inject = ['$state', 'myLineageService'];

angular.module(PKG.name + '.feature.tracker')
  .service('LineageStore', LineageStore);
