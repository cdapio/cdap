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
  constructor($state, myJumpFactory, rDatasetType, rSystemTags) {
    this.$state = $state;
    this.myJumpFactory = myJumpFactory;

    let entityParams = this.$state.params.entityType;
    let entitySplit = entityParams.split(':');

    this.datasetType = rDatasetType;
    this.datasetExplorable = Array.isArray(rSystemTags) && rSystemTags.indexOf('explore') !== -1;

    // Mock data
    this.truthMeterData = {
      datasets: [
        {
          name: 'history',
          value: 80
        },
        {
          name: 'purchases',
          value: 26
        }
      ],
      streams: [
        {
          name: 'purchaseStream',
          value: 44
        },
        {
          name: 'strm2',
          value: 99
        }
      ]
    };

    switch (entitySplit[0]) {
      case 'streams':
        this.entityInfo = {
          name: 'Stream',
          icon: 'icon-streams'
        };
        for (var i=0; i < this.truthMeterData.streams.length; i++) {
          if(this.truthMeterData.streams[i].name === this.$state.params.entityId) {
            this.entityInfo.score = this.truthMeterData.streams[i].value;
          }
        }
        break;
      case 'datasets':
        this.entityInfo = {
          name: 'Dataset',
          icon: 'icon-datasets'
        };
        for (var n=0; n < this.truthMeterData.datasets.length; n++) {
          if(this.truthMeterData.datasets[n].name === this.$state.params.entityId) {
            this.entityInfo.score = this.truthMeterData.datasets[n].value;
          }
        }
        break;
      case 'views':
        this.entityInfo = {
          name: 'Stream View',
          icon: 'icon-streams'
        };
        for (var x=0; x < this.truthMeterData.streams.length; x++) {
          if(this.truthMeterData.streams[x].name === this.$state.params.entityId) {
            this.entityInfo.score = this.truthMeterData.streams[x].value;
          }
        }
        break;
    }
  }

  goBack() {
    this.$state.go('tracker.detail.result', {
      namespace: this.$state.params.namespace,
      searchQuery: this.$state.params.searchTerm
    });
  }
}

TrackerEntityController.$inject = ['$state', 'myJumpFactory', 'rDatasetType', 'rSystemTags'];

angular.module(PKG.name + '.feature.tracker')
 .controller('TrackerEntityController', TrackerEntityController);
