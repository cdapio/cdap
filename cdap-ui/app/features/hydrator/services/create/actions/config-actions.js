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
  constructor(ConfigDispatcher, myPipelineApi, $state, ConfigStore, mySettings, ConsoleActionsFactory, HydratorErrorFactory, EventPipe) {
    this.ConfigStore = ConfigStore;
    this.mySettings = mySettings;
    this.$state = $state;
    this.myPipelineApi = myPipelineApi;
    this.ConsoleActionsFactory = ConsoleActionsFactory;
    this.HydratorErrorFactory = HydratorErrorFactory;
    this.EventPipe = EventPipe;

    this.dispatcher = ConfigDispatcher.getDispatcher();
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
  savePlugin(plugin, type) {
    this.dispatcher.dispatch('onPluginSave', {plugin: plugin, type: type});
  }
  saveAsDraft(config) {
    this.dispatcher.dispatch('onSaveAsDraft', config);
  }
  setArtifact(artifact) {
    this.dispatcher.dispatch('onArtifactSave', artifact);
  }
  addPlugin (plugin, type) {
    this.dispatcher.dispatch('onPluginAdd', plugin, type);
  }
  editPlugin(pluginId, pluginProperties) {
    this.dispatcher.dispatch('onPluginEdit', pluginId, pluginProperties);
  }
  setSchedule(schedule) {
    this.dispatcher.dispatch('onSetSchedule', schedule);
  }
  setInstance(instance) {
    this.dispatcher.dispatch('onSetInstance', instance);
  }
  publishPipeline() {

    var error = this.HydratorErrorFactory.isModelValid();

    if (!error) { return; }
    this.EventPipe.emit('showLoadingIcon', 'Publishing Pipeline to CDAP');

    let removeFromUserDrafts = (adapterName) => {
      this.mySettings
          .get('adapterDrafts')
          .then(
            (res) => {
              if (angular.isObject(res)) {
                delete res[adapterName];
                return this.mySettings.set('adapterDrafts', res);
              }
            },
            (err) => {
              this.ConsoleActionsFactory.addMessage({
                type: 'error',
                content: err
              });
              return this.$q.reject(false);
            }
          )
          .then(
            () => {
              this.EventPipe.emit('hideLoadingIcon.immediate');
              this.$state.go('hydrator.list');
            }
          );
    };

    var config = this.ConfigStore.getConfigForExport();
    this.myPipelineApi.save(
      {
        namespace: this.$state.params.namespace,
        pipeline: config.name
      },
      config
    )
      .$promise
      .then(
        removeFromUserDrafts.bind(this, config.name),
        (err) => {
          this.EventPipe.emit('hideLoadingIcon.immediate');
          this.ConsoleActionsFactory.addMessage({
            type: 'error',
            content: err
          });
          // this.notifyError({
          //   canvas: [err.data]
          // });
        }
      );
  }
}

ConfigActionsFactory.$inject = ['ConfigDispatcher', 'myPipelineApi', '$state', 'ConfigStore', 'mySettings', 'ConsoleActionsFactory', 'HydratorErrorFactory', 'EventPipe'];
angular.module(`${PKG.name}.feature.hydrator`)
  .service('ConfigActionsFactory', ConfigActionsFactory);
