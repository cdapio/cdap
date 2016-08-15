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
  constructor($state, myTrackerApi, $scope, LineageActions, LineageStore) {
    this.$state = $state;
    this.myTrackerApi = myTrackerApi;
    this.$scope = $scope;
    this.LineageActions = LineageActions;
    LineageStore.setDefaults();

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

    this.customTimeRange = {
      startTime: null,
      endTime: null
    };

    this.timeRange = {
      start: $state.params.start || 'now-7d',
      end: $state.params.end || 'now'
    };

    this.selectedTimeRange = this.findTimeRange();
    this.getLineage(this.$state.params.entityType, this.$state.params.entityId);
  }

  findTimeRange() {
    let match = this.timeRangeOptions.filter( (option) => {
      return option.start === this.timeRange.start && option.end === this.timeRange.end;
    });

    if (match.length === 0) {
      this.isCustom = true;
      this.customTimeRange.startTime = new Date(parseInt(this.$state.params.start, 10) * 1000);
      this.customTimeRange.endTime = new Date(parseInt(this.$state.params.end, 10) * 1000);
    }

    return match.length > 0 ? match[0] : { label: 'Custom' };
  }

  goToCustomTimeRangeEntityDetailView() {
    let startTime = parseInt(this.customTimeRange.startTime.valueOf() / 1000, 10);
    let endTime = parseInt(this.customTimeRange.endTime.valueOf() / 1000, 10);

    this.$state.go('tracker.detail.entity.lineage', { start: startTime, end: endTime });
  }

  selectCustom() {
    this.isCustom = true;
    this.selectedTimeRange.label = 'Custom';
  }

  getLineage(entityType, entityId) {
    this.loading = true;
    let params = {
      namespace: this.$state.params.namespace,
      entityType: entityType,
      entityId: entityId,
      scope: this.$scope,
      start: this.timeRange.start,
      end: this.timeRange.end,
      levels: 1
    };

    this.myTrackerApi.getLineage(params)
      .$promise
      .then((res) => {
        this.LineageActions.loadLineageData(res, params, this.$state.params.method);
        this.loading = false;
      }, (err) => {
        console.log('Error', err);
        this.loading = false;
      });
  }
}

TrackerLineageController.$inject = ['$state', 'myTrackerApi', '$scope', 'LineageActions', 'LineageStore'];

angular.module(PKG.name + '.feature.tracker')
 .controller('TrackerLineageController', TrackerLineageController);
