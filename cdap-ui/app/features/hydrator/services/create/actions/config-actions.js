class ConfigActionsFactory {
  constructor(ConfigDispatcher) {
    this.dispatcher = ConfigDispatcher.getDispatcher();
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
