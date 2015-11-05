class PluginActionsFactory {
  constructor(PluginsDispatcher, myPipelineApi, PluginConfigFactory) {
    this.dispatcher = PluginsDispatcher.getDispatcher();
    this.api = myPipelineApi;
    this.pluginConfigApi = PluginConfigFactory;
  }
  fetchSources(params) {
    this.api
        .fetchSource(params)
        .$promise
        .then(
          res => this.dispatcher.dispatch('onSourcesFetch', res),
          err => this.dispatcher.dispatch('onSourceFetch', {err: err})
        );
  }
  fetchSinks(params) {
    this.api
        .fetchSinks(params)
        .$promise
        .then(
          res => this.dispatcher.dispatch('onSinksFetch', res),
          err => this.dispatcher.dispatch('onSinksFetch', {err: err})
        );
  }
  fetchTransforms(params) {
    this.api
        .fetchTransforms(params)
        .$promise
        .then(
          res => this.dispatcher.dispatch('onTransformsFetch', res),
          err => this.dispatcher.dispatch('onTransformsFetch', {err: err})
        );
  }
  fetchProperties(params) {
    this.api
        .fetchPluginProperties(params)
        .$promise
        .then(
          res => this.dispatcher.dispatch('onPluginPropertiesFetch', {properties: res, params: params}),
          err => this.dispatcher.dispatch('onPluginPropertiesFetch', {err: err, params: params})
        );
  }
  fetchNodeConfig(params) {
    let requiredParams = {
      templateid: params.templateid,
      pluginid: params.pluginid
    };
    this.pluginConfigApi
        .fetch(params.scope, params.templateid, params.pluginid)
        .then(
          res => this.dispatcher.dispatch('onPluginConfigFetch', {config: res, params: requiredParams}),
          err => this.dispatcher.dispatch('onPluginConfigFetch', {err: err, params: requiredParams})
        );
  }
}

PluginActionsFactory.$inject = ['PluginsDispatcher', 'myPipelineApi', 'PluginConfigFactory'];
angular.module(`${PKG.name}.feature.hydrator`)
  .service('PluginActionsFactory', PluginActionsFactory);
