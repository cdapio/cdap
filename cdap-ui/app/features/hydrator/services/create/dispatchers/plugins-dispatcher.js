class PluginsDispatcher {
  constructor(CaskAngularDispatcher) {
    this.__dispatcher__ = null;
    this.baseDispatcher = CaskAngularDispatcher;
  }
  getDispatcher() {
    if (!this.__dispatcher__) {
       this.__dispatcher__ = new this.baseDispatcher();
    }
    return this.__dispatcher__;
  }
  destroyDispatcher() {
    delete this.__dispatcher__;
  }
}
PluginsDispatcher.$inject = ['CaskAngularDispatcher'];
angular.module(`${PKG.name}.feature.hydrator`)
  .service('PluginsDispatcher', PluginsDispatcher);
