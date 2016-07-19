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
  constructor($state, myJumpFactory, rDatasetType, rSystemTags, $scope, myTrackerApi) {
    'ngInject';

    this.$state = $state;
    this.$scope = $scope;
    this.myJumpFactory = myJumpFactory;
    this.myTrackerApi = myTrackerApi;

    let entityParams = this.$state.params.entityType;
    let entitySplit = entityParams.split(':');

    this.datasetType = rDatasetType;
    this.datasetExplorable = Array.isArray(rSystemTags) && rSystemTags.indexOf('explore') !== -1;

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

    this.fetchTruthMeter();
  }

  fetchTruthMeter() {
    let params = {
      namespace: this.$state.params.namespace,
      scope: this.$scope
    };

    let datasetsList = [];
    let streamsList = [];

    if (this.entityInfo.name === 'Stream') {
      streamsList.push(this.$state.params.entityId);
    } else if (this.entityInfo.name === 'Dataset') {
      datasetsList.push(this.$state.params.entityId);
    } else {
      return;
    }

    this.myTrackerApi.getTruthMeter(params, {
      streams: streamsList,
      datasets: datasetsList
    })
      .$promise
      .then((res) => {
        this.truthMeterMap = res;
      }, (err) => {
        console.log('error', err);
      });
  }

  goBack() {
    this.$state.go('tracker.detail.result', {
      namespace: this.$state.params.namespace,
      searchQuery: this.$state.params.searchTerm
    });
  }
}

TrackerEntityController.$inject = ['$state', 'myJumpFactory', 'rDatasetType', 'rSystemTags', '$scope', 'myTrackerApi'];

angular.module(PKG.name + '.feature.tracker')
 .controller('TrackerEntityController', TrackerEntityController);
