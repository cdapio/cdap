angular.module(PKG.name + '.feature.hydrator')
  .controller('HydratorCreateCanvasController', function(BottomPanelStore) {
    this.setState = function() {
      this.state = {
        setScroll: (BottomPanelStore.getPanelState() === 0? false: true)
      };
    };
    this.setState();
    BottomPanelStore.registerOnChangeListener(this.setState.bind(this));
  });
