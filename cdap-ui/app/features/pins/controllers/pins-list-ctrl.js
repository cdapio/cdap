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
