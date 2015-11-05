class LeftPanelStore {
  constructor(LeftPanelDispatcher) {
    this.state = {};
    this.setDefaults();
    let dispatcher = LeftPanelDispatcher.getDispatcher();
    dispatcher.register('onLeftPanelToggled', this.setState.bind(this));
  }
  setDefaults() {
    this.state = {
      panelState: 1
    };
  }
  getState() {
    return this.state.panelState;
  }
  setState(state) {
    this.state = state;
  }
}

LeftPanelStore.$inject = ['LeftPanelDispatcher'];
angular.module(`${PKG.name}.feature.hydrator`)
  .service('LeftPanelStore', LeftPanelStore);
