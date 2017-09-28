/*
 * Copyright © 2015-2017 Cask Data, Inc.
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

class HydratorPlusPlusConfigActions {
  constructor(HydratorPlusPlusConfigDispatcher) {
    'ngInject';
    this.dispatcher = HydratorPlusPlusConfigDispatcher.getDispatcher();
  }
  initializeConfigStore(config) {
    this.dispatcher.dispatch('onInitialize', config);
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
  saveAsDraft(config) {
    this.dispatcher.dispatch('onSaveAsDraft', config);
  }
  setEngine(engine) {
    this.dispatcher.dispatch('onEngineChange', engine);
  }
  editPlugin(pluginId, pluginProperties) {
    this.dispatcher.dispatch('onPluginEdit', pluginId, pluginProperties);
  }
  propagateSchemaDownStream(pluginId) {
    this.dispatcher.dispatch('onSchemaPropagationDownStream', pluginId);
  }
  setSchedule(schedule) {
    this.dispatcher.dispatch('onSetSchedule', schedule);
  }
  setInstance(instance) {
    this.dispatcher.dispatch('onSetInstance', instance);
  }
  setBatchInterval(batchInterval) {
    this.dispatcher.dispatch('onSetBatchInterval', batchInterval);
  }
  setVirtualCores(virtualCores) {
    this.dispatcher.dispatch('onSetVirtualCores', virtualCores);
  }
  setMemoryMB(memoryMB) {
    this.dispatcher.dispatch('onSetMemoryMB', memoryMB);
  }
  setDriverVirtualCores(virtualCores) {
    this.dispatcher.dispatch('onSetDriverVirtualCores', virtualCores);
  }
  setDriverMemoryMB(memoryMB) {
    this.dispatcher.dispatch('onSetDriverMemoryMB', memoryMB);
  }
  setClientVirtualCores(virtualCores) {
    this.dispatcher.dispatch('onSetClientVirtualCores', virtualCores);
  }
  setClientMemoryMB(memoryMB) {
    this.dispatcher.dispatch('onSetClientMemoryMB', memoryMB);
  }
  addPostAction(config) {
    this.dispatcher.dispatch('onAddPostAction', config);
  }
  editPostAction(config) {
    this.dispatcher.dispatch('onEditPostAction', config);
  }
  deletePostAction(config) {
    this.dispatcher.dispatch('onDeletePostAction', config);
  }
  setMaxConcurrentRuns(num) {
    this.dispatcher.dispatch('onSetMaxConcurrentRuns', num);
  }
  publishPipeline() {
    this.dispatcher.dispatch('onPublishPipeline');
  }
}

angular.module(`${PKG.name}.feature.hydrator`)
  .service('HydratorPlusPlusConfigActions', HydratorPlusPlusConfigActions);
