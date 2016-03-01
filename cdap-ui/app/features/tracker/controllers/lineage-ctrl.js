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

class TrackerLineageController{
  constructor($state, myTrackerApi, $scope, myLineageService) {
    this.$state = $state;
    this.myTrackerApi = myTrackerApi;
    this.$scope = $scope;
    this.myLineageService = myLineageService;

    this.timeRangeOptions = [
      {
        label: 'Last 7 days',
        start: 'now-7d',
        end: 'now'
      },
      {
        label: 'Last 14 days',
        start: 'now-14d',
        end: 'now'
      },
      {
        label: 'Last month',
        start: 'now-30d',
        end: 'now'
      },
      {
        label: 'Last 6 months',
        start: 'now-180d',
        end: 'now'
      },
      {
        label: 'Last 12 months',
        start: 'now-365d',
        end: 'now'
      }
    ];

    this.lineageInfo = {};
    this.loading = false;

    this.selectTimeRange(this.timeRangeOptions[0]);
  }

  selectTimeRange (time) {
    this.timeRange = time;
    this.getLineage();
  }

  getLineage() {
    this.loading = true;
    let params = {
      namespace: this.$state.params.namespace,
      entityType: this.$state.params.entityType,
      entityId: this.$state.params.entityId,
      scope: this.$scope,
      start: this.timeRange.start,
      end: this.timeRange.end,
      levels: 2
    };

    this.myTrackerApi.getLineage(params)
      .$promise
      .then((res) => {
        this.lineageInfo = this.myLineageService.parseLineageResponse(res);
        this.loading = false;
      }, (err) => {
        console.log('Error', err);
        this.loading = false;
      });
  }
}

TrackerLineageController.$inject = ['$state', 'myTrackerApi', '$scope', 'myLineageService'];

angular.module(PKG.name + '.feature.tracker')
 .controller('TrackerLineageController', TrackerLineageController);
