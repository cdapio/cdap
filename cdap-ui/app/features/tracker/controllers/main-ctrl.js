/*
 * Copyright © 2016 Cask Data, Inc.
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

class TrackerMainController{
  constructor($state) {
    this.$state = $state;
    this.searchQuery = '';

    // Placeholder data: TODO: remove once backend API is ready
    this.topAppsData = {
      'total' : 15,
      'results' : [
        {
          'label' : 'Application1',
          'value' : 36
        },
        {
          'label' : 'Application3',
          'value' : 28
        },
        {
          'label' : 'Application2',
          'value' : 17
        },
        {
          'label' : 'Application5',
          'value' : 17
        },
        {
          'label' : 'Application4',
          'value' : 1
        }
      ]
    };
  }

  search(event) {
    if (event.keyCode === 13 && this.searchQuery) {
      this.$state.go('tracker.detail.result', { searchQuery: this.searchQuery });
    }
  }
}

TrackerMainController.$inject = ['$state'];

angular.module(PKG.name + '.feature.tracker')
  .controller('TrackerMainController', TrackerMainController);
