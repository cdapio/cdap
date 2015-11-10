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

class ConfigActionsFactory {
  constructor(ConfigDispatcher) {
    this.dispatcher = ConfigDispatcher.getDispatcher();
  }
  setMetadataInfo(name, description) {
    this.dispatcher.dispatch('onMetadataInfoSave', name, description);
  }
  setDescription(description) {
    this.dispatcher.dispatch('onDescriptionSave', description);
  }
  setConfig(config) {
    this.dispatcher.dispatch('onConfigSave', config);
  }
  savePlugin(plugin, type) {
    this.dispatcher.dispatch('onPluginSave', {plugin: plugin, type: type});
  }
  setArtifact(artifact) {
    this.dispatcher.dispatch('onArtifactSave', artifact);
  }
  addPlugin (plugin, type) {
    this.dispatcher.dispatch('onPluginAdd', {plugin: plugin, type: type});
  }
}

ConfigActionsFactory.$inject = ['ConfigDispatcher'];
angular.module(`${PKG.name}.feature.hydrator`)
  .service('ConfigActionsFactory', ConfigActionsFactory);
