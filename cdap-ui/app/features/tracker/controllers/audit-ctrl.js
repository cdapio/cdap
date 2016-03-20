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

class TrackerAuditController {
  constructor($state, $scope, myTrackerApi, myAlertOnValium) {

    this.$state = $state;
    this.$scope = $scope;
    this.myTrackerApi = myTrackerApi;
    this.myAlertOnValium = myAlertOnValium;

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

    this.timeRange = {
      start: $state.params.start || 'now-7d',
      end: $state.params.end || 'now'
    };

    this.selectedTimeRange = this.findTimeRange();

    this.currentPage = 1;
    this.auditLogs = [];
    this.fetchAuditLogs(this.currentPage);
  }

  findTimeRange() {
    let match = this.timeRangeOptions.filter( (option) => {
      return option.start === this.timeRange.start && option.end === this.timeRange.end;
    });
    return match.length > 0 ? match[0] : { label: 'Custom' };
  }

  fetchAuditLogs(currentPage) {
    let params = {
      namespace: this.$state.params.namespace,
      entityType: this.$state.params.entityType === 'streams' ? 'stream' : 'dataset',
      entityId: this.$state.params.entityId,
      startTime: this.timeRange.start,
      endTime: this.timeRange.end,
      offset: (currentPage - 1) * 10,
      scope: this.$scope
    };

    this.myTrackerApi.getAuditLogs(params)
      .$promise
      .then((response) => {
        this.auditLogs = response;
      }, (err) => {
        console.log('Error: ', err);
        this.myAlertOnValium.show({
          type: 'danger',
          content: 'Enable the Tracker extension to view audit logs.'
        });
      });
  }
}

TrackerAuditController.$inject = ['$state', '$scope', 'myTrackerApi', 'myAlertOnValium'];

angular.module(PKG.name + '.feature.tracker')
.controller('TrackerAuditController', TrackerAuditController);

