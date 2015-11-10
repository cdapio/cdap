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
        this.state.source = config;
        break;
      case 'sink':
        this.state.sinks.push(config);
        break;
      case 'transform':
        this.state.transforms.push(config);
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
}

ConfigStore.$inject = ['ConfigDispatcher'];
angular.module(`${PKG.name}.feature.hydrator`)
  .service('ConfigStore', ConfigStore);
