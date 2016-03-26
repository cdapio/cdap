/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the 'License'); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an 'AS IS' BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

class TrackerEntityController{
  constructor($state, myJumpFactory, rDatasetType) {
    this.$state = $state;
    this.myJumpFactory = myJumpFactory;

    let entityParams = this.$state.params.entityType;
    let entitySplit = entityParams.split(':');

    this.datasetType = rDatasetType;

    switch (entitySplit[0]) {
      case 'streams':
        this.entityInfo = {
          name: 'Stream',
          icon: 'icon-streams'
        };
        break;
      case 'datasets':
        this.entityInfo = {
          name: 'Dataset',
          icon: 'icon-datasets'
        };
        break;
      case 'views':
        this.entityInfo = {
          name: 'Stream View',
          icon: 'icon-streams'
        };
        break;
    }

  }

  goBack() {
    this.$state.go('tracker.result', {
      namespace: this.$state.params.namespace,
      searchQuery: this.$state.params.searchTerm
    });
  }
}

TrackerEntityController.$inject = ['$state', 'myJumpFactory', 'rDatasetType'];

angular.module(PKG.name + '.feature.tracker')
 .controller('TrackerEntityController', TrackerEntityController);
