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

class TrackerUsageController {
  constructor($state, $scope, myTrackerApi) {

    this.$state = $state;
    this.$scope = $scope;
    this.myTrackerApi = myTrackerApi;

    let entitySplit = this.$state.params.entityType.split(':');

    this.entityType = entitySplit;

    this.timeRangeOptions = [
      {
        label: 'Last 7 days',
        startTime: 'now-7d',
        endTime: 'now'
      },
      {
        label: 'Last 14 days',
        startTime: 'now-14d',
        endTime: 'now'
      },
      {
        label: 'Last month',
        startTime: 'now-30d',
        endTime: 'now'
      },
      {
        label: 'Last 6 months',
        startTime: 'now-180d',
        endTime: 'now'
      },
      {
        label: 'Last 12 months',
        startTime: 'now-365d',
        endTime: 'now'
      }
    ];

    this.timeRange = {
      startTime: $state.params.startTime || 'now-7d',
      endTime: $state.params.endTime || 'now'
    };

    this.customTimeRange = {
      startTime: null,
      endTime: null
    };

    this.selectedTimeRange = this.findTimeRange();

    this.fetchAuditHistogram();
    this.fetchTopDatasets();
    this.fetchTimeSince();
  }

  findTimeRange() {
    let match = this.timeRangeOptions.filter( (option) => {
      return option.startTime === this.timeRange.startTime && option.endTime === this.timeRange.endTime;
    });

    if (match.length === 0) {
      this.isCustom = true;
      this.customTimeRange.startTime = new Date(parseInt(this.$state.params.startTime, 10) * 1000);
      this.customTimeRange.endTime = new Date(parseInt(this.$state.params.endTime, 10) * 1000);
    }

    return match.length > 0 ? match[0] : { label: 'Custom' };
  }

  goToCustomTimeRangeEntityDetailView() {
    let startTime = parseInt(this.customTimeRange.startTime.valueOf() / 1000, 10);
    let endTime = parseInt(this.customTimeRange.endTime.valueOf() / 1000, 10);

    this.$state.go('tracker.detail.entity.usage', { startTime: startTime, endTime: endTime });
  }

  selectCustom() {
    this.isCustom = true;
    this.selectedTimeRange.label = 'Custom';
  }

  fetchAuditHistogram() {
    let params = {
      namespace: this.$state.params.namespace,
      startTime: this.timeRange.startTime,
      endTime: this.timeRange.endTime,
      scope: this.$scope,
      entityName: this.$state.params.entityId,
      entityType: this.$state.params.entityType === 'streams' ? 'stream' : 'dataset'
    };

    this.myTrackerApi.getAuditHistogram(params)
      .$promise
      .then((response) => {
        this.auditHistogram = response;
        this.serviceUnavailable = false;
      }, (err) => {
        if (err.statusCode === 503) {
          this.serviceUnavailable = true;
        }
        console.log('Error', err);
      });
  }

  fetchTopDatasets() {
    let params = {
      namespace: this.$state.params.namespace,
      limit: 5,
      scope: this.$scope,
      entity: 'datasets'
    };

    this.myTrackerApi.getTopEntities(params)
      .$promise
      .then((response) => {
        this.topDatasets = response;
      }, (err) => {
        console.log('Error', err);
      });
  }

  fetchTimeSince() {
    let params = {
      namespace: this.$state.params.namespace,
      scope: this.$scope,
      entityName: this.$state.params.entityId,
      entityType: this.$state.params.entityType === 'streams' ? 'stream' : 'dataset'
    };

    this.myTrackerApi.getTimeSince(params)
      .$promise
      .then((response) => {
        this.timeSince = response;
        this.serviceUnavailable = false;
      }, (err) => {
        if (err.statusCode === 503) {
          this.serviceUnavailable = true;
        }
        console.log('Error', err);
      });
  }
}

TrackerUsageController.$inject = ['$state', '$scope', 'myTrackerApi'];

angular.module(PKG.name + '.feature.tracker')
.controller('TrackerUsageController', TrackerUsageController);

