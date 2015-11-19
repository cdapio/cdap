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

class ConfigStore {
  constructor(ConfigDispatcher){
    this.state = {};
    this.changeListeners = [];
    this.setDefaults();
    this.configDispatcher = ConfigDispatcher.getDispatcher();
    this.configDispatcher.register('onArtifactSave', this.setArtifact.bind(this));
    this.configDispatcher.register('onMetadataInfoSave', this.setMetadataInformation.bind(this));
    this.configDispatcher.register('onPluginAdd', this.setConfig.bind(this));
  }
  registerOnChangeListener(callback) {
    this.changeListeners.push(callback);
  }
  emitChange() {
    this.changeListeners.forEach( callback => callback() );
  }
  setDefaults() {
    this.state = {
      artifact: {
        name: '',
        scope: 'SYSTEM',
        version: ''
      },
      config: {
        source: {},
        sinks: [],
        transforms: []
      },
      nodes: [],
      connections: [],
      description: '',
      name: ''
    };
  }

  setState(state) {
    this.state = state;
  }
  getState() {
    return this.state;
  }
  getArtifact() {
    return this.state.artifact;
  }
  getAppType() {
    return this.state.artifact.name;
  }
  getConnections() {
    return this.state.connections;
  }
  getConfig() {
    return this.state.config;
  }
  getDescription() {
    return this.state.description;
  }
  getName() {
    return this.state.name;
  }
  setName(name) {
    this.state.name = name;
    this.emitChange();
  }
  setDescription(description) {
    this.state.description = description;
    this.emitChange();
  }
  setMetadataInformation(name, description) {
    this.state.name = name;
    this.state.description = description;
    this.emitChange();
  }
  setConfig(config, type) {
    switch(type) {
      case 'source':
        this.state.config.source = config;
        break;
      case 'sink':
        this.state.config.sinks.push(config);
        break;
      case 'transform':
        this.state.config.transforms.push(config);
        break;
    }
    this.emitChange();
  }
  setArtifact(artifact) {
    this.state.artifact.name = artifact.name;
    this.state.artifact.version = artifact.version;
    this.state.artifact.scope = artifact.scope;
    this.emitChange();
  }
  setNodes(nodes) {
    this.state.nodes = nodes;
  }
  setConnections(connections) {
    this.state.connections = connections;
  }
  addNode(node) {
    this.state.nodes.push(node);
  }
  getNodes() {
    return this.state.nodes;
  }
  getNode(nodeId) {
    var nodes = this.state.nodes;
    var match = nodes.filter( node => node.id === nodeId);
    if (match.length) {
      match = match[0];
    } else {
      match = null;
    }
    return match;
  }
}

ConfigStore.$inject = ['ConfigDispatcher'];
angular.module(`${PKG.name}.feature.hydrator`)
  .service('ConfigStore', ConfigStore);
