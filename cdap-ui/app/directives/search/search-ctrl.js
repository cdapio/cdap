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
 
angular.module(PKG.name + '.commons')
  .controller('MySearchCtrl', function($state, mySettings, $alert) {
    this.searchTxt = '';
    this.mode = 'SEARCH';

    this.onSearch = function(event) {
      if (event.keyCode === 13) {
        switch(this.mode) {
          case 'SEARCH':
            $state.go('search.list', {searchTag: this.searchTxt});
            break;
          case 'PIN':
            mySettings.get('user-pins', true)
              .then(
                function(pins) {
                  if(!pins) {
                    pins = [];
                  }
                  pins.push({
                    label: event.target.value,
                    name: $state.current.name,
                    params: $state.params
                  });
                  return mySettings.set('user-pins', pins);
                }
              )
              .then(
                function success() {
                  $alert({
                    type: 'success',
                    content: 'Pin ' + event.target.value + ' saved successfully'
                  });
                },
                function error() {
                  $alert({
                    type: 'danger',
                    content: 'Pin ' + event.target.value + ' could not be saved'
                  });
                }
              );
        }
      }

    };
  });
