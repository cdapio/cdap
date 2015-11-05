class ConfigDispatcher {
  constructor(CaskAngularDispatcher) {
    this.baseDispatcher = CaskAngularDispatcher;
    this.__dispatcher__ = null;
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
ConfigDispatcher.$inject = ['CaskAngularDispatcher'];
angular.module(`${PKG.name}.feature.hydrator`)
  .service('ConfigDispatcher', ConfigDispatcher);
