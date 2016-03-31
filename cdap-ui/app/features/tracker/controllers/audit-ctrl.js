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
  constructor($state, $scope, myTrackerApi, myAlertOnValium, $q) {

    this.$state = $state;
    this.$scope = $scope;
    this.myTrackerApi = myTrackerApi;
    this.myAlertOnValium = myAlertOnValium;
    this.$q = $q;

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

    this.customTimeRange = {
      startTime: null,
      endTime: null
    };

    this.isTrackerSetup = true;
    this.enableTrackerLoading = false;

    this.selectedTimeRange = this.findTimeRange();

    this.currentPage = 1;
    this.auditLogs = [];
    this.fetchAuditLogs(this.currentPage);
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

  goCustomDate() {
    let startTime = parseInt(this.customTimeRange.startTime.valueOf() / 1000, 10);
    let endTime = parseInt(this.customTimeRange.endTime.valueOf() / 1000, 10);

    this.$state.go('tracker.entity.audit', { start: startTime, end: endTime });
  }

  fetchAuditLogs(currentPage) {
    this.enableTrackerLoading = false;
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
        this.isTrackerSetup = true;
        this.auditLogs = response;
      }, (err) => {
        if (err.statusCode === 503) {
          this.isTrackerSetup = false;
        } else {
          this.myAlertOnValium.show({
            type: 'danger',
            content: err.data
          });
        }
      });
  }

  enableTracker() {
    this.enableTrackerLoading = true;
    this.myTrackerApi.getTrackerApp({
      namespace: this.$state.params.namespace,
      scope: this.$scope
    })
    .$promise
    .then(() => {
      // start programs
      this.startPrograms();
    }, () => {
      // create app
      this.createTrackerApp();
    });
  }

  createTrackerApp() {
    this.myTrackerApi.getCDAPConfig({ scope: this.$scope })
      .$promise
      .then((res) => {
        let zookeeper = res.filter( (c) => {
          return c.name === 'zookeeper.quorum';
        })[0].value;

        let kafkaNamespace = res.filter( (c) => {
          return c.name === 'kafka.zookeeper.namespace';
        })[0].value;

        let numPartitions = res.filter( (c) => {
          return c.name === 'kafka.num.partitions';
        })[0].value;

        let topic = res.filter( (c) => {
          return c.name === 'audit.kafka.topic';
        })[0].value;

        let config = {
          artifact: {
            name: 'tracker',
            version: '1.0-SNAPSHOT',
            scope: 'USER'
          },
          config: {
            auditLogKafkaConfig: {
              zookeeperString: zookeeper + '/' + kafkaNamespace,
              topic: topic,
              numPartitions: numPartitions
            }
          }
        };

        this.myTrackerApi.deployTrackerApp({
          namespace: this.$state.params.namespace,
          scope: this.$scope
        }, config)
          .$promise
          .then(() => {
            this.startPrograms();
          });
      });

  }

  startPrograms() {
    let auditServiceParams = {
      namespace: this.$state.params.namespace,
      programType: 'services',
      programId: 'AuditLog',
      scope: this.$scope
    };

    let auditFlowParams = {
      namespace: this.$state.params.namespace,
      programType: 'flows',
      programId: 'AuditLogFlow',
      scope: this.$scope
    };

    this.$q.all([
      this.myTrackerApi.startTrackerProgram(auditServiceParams, {}).$promise,
      this.myTrackerApi.startTrackerProgram(auditFlowParams, {}).$promise
    ]).then( () => {
      this.fetchAuditLogs(this.currentPage);
    }, () => {
      // it errors out if one of the programs already started
      this.fetchAuditLogs(this.currentPage);
    });
  }
}

TrackerAuditController.$inject = ['$state', '$scope', 'myTrackerApi', 'myAlertOnValium', '$q'];

angular.module(PKG.name + '.feature.tracker')
.controller('TrackerAuditController', TrackerAuditController);

