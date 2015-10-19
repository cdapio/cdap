
class PinsListController {
  constructor(mySettings, $state, caskFocusManager) {
    this.pinsList = [];

    this.gridsterOpts = {
      rowHeight: '40',
      columns: 12,
      minSizeX: 2,
      swapping: false,
      draggable: {
        enabled: false
      },
      resizable: {
        enabled: false
      }
    };
    this.$state = $state;
    caskFocusManager.select('searchPinsText');

    mySettings.get('user-pins')
      .then(
        (pins) => {
          this.pinsList = pins;
          this.pinsList.forEach( (pin) => {
            pin.href = this.$state.href(pin.name, pin.params);
          });
        }
      );
  }
}
PinsListController.$inject = ['mySettings', '$state', 'caskFocusManager'];

angular.module(`${PKG.name}.feature.pins`)
  .controller('PinsListController', PinsListController);
