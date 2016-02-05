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
  constructor(ConfigDispatcher, myPipelineApi, $state, ConfigStore, mySettings, ConsoleActionsFactory, EventPipe, myAppsApi, GLOBALS, myHelpers, $stateParams) {
    this.ConfigStore = ConfigStore;
    this.mySettings = mySettings;
    this.$state = $state;
    this.myPipelineApi = myPipelineApi;
    this.ConsoleActionsFactory = ConsoleActionsFactory;
    this.EventPipe = EventPipe;
    this.myAppsApi = myAppsApi;
    this.GLOBALS = GLOBALS;
    this.myHelpers = myHelpers;
    this.$stateParams = $stateParams;

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
  propagateSchemaDownStream(pluginId) {
    this.dispatcher.dispatch('onSchemaPropagationDownStream', pluginId);
  }
  setSchedule(schedule) {
    this.dispatcher.dispatch('onSetSchedule', schedule);
  }
  setInstance(instance) {
    this.dispatcher.dispatch('onSetInstance', instance);
  }
  publishPipeline() {
    this.ConsoleActionsFactory.resetMessages();
    let error = this.ConfigStore.validateState(true);

    if (!error) { return; }
    this.EventPipe.emit('showLoadingIcon', 'Publishing Pipeline to CDAP');

    let removeFromUserDrafts = (adapterName) => {
      let draftId = this.ConfigStore.getState().__ui__.draftId;
      this.mySettings
        .get('hydratorDrafts', true)
        .then(
          (res) => {
            var savedDraft = this.myHelpers.objectQuery(res, this.$stateParams.namespace, draftId);
            if (savedDraft) {
              delete res[this.$stateParams.namespace][draftId];
              return this.mySettings.set('hydratorDrafts', res);
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
            this.$state.go('hydrator.detail', { pipelineId: adapterName });
          }
        );
    };

    let publish = (pipelineName) => {
      this.myPipelineApi.save(
        {
          namespace: this.$state.params.namespace,
          pipeline: pipelineName
        },
        config
      )
      .$promise
      .then(
        removeFromUserDrafts.bind(this, pipelineName),
        (err) => {
          this.EventPipe.emit('hideLoadingIcon.immediate');
          this.ConsoleActionsFactory.addMessage({
            type: 'error',
            content: angular.isObject(err) ? err.data : err
          });
        }
      );
    };


    var config = this.ConfigStore.getConfigForExport();

    // Checking if Pipeline name already exist
    this.myAppsApi
      .list({ namespace: this.$state.params.namespace })
      .$promise
      .then( (apps) => {
        var appNames = apps.map( (app) => { return app.name; } );

        if (appNames.indexOf(config.name) !== -1) {
          this.ConsoleActionsFactory.addMessage({
            type: 'error',
            content: this.GLOBALS.en.hydrator.studio.error['NAME-ALREADY-EXISTS']
          });
          this.EventPipe.emit('hideLoadingIcon.immediate');
        } else {
          publish(config.name);
        }
      });

  }
}

ConfigActionsFactory.$inject = ['ConfigDispatcher', 'myPipelineApi', '$state', 'ConfigStore', 'mySettings', 'ConsoleActionsFactory', 'EventPipe', 'myAppsApi', 'GLOBALS', 'myHelpers', '$stateParams'];
angular.module(`${PKG.name}.feature.hydrator`)
  .service('ConfigActionsFactory', ConfigActionsFactory);
