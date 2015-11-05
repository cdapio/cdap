class LeftPanelActionsFactory {
  constructor(LeftPanelDispatcher) {
    this.dispatcher = LeftPanelDispatcher.getDispatcher();
  }
  expand() {
    this.dispatcher.dispatch('OnLeftPanelToggled', {panelState: 1});
  }
  minimize() {
    this.dispatcher.dispatch('OnLeftPanelToggled', {panelState: 0});
  }
}

LeftPanelActionsFactory.$inject = ['LeftPanelDispatcher'];
angular.module(`${PKG.name}.feature.hydrator`)
  .service('LeftPanelActionsFactory', LeftPanelActionsFactory);
