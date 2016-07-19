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

class TrackerEnableController{
  constructor($state, myTrackerApi, $scope, myAlertOnValium, $q, UI_CONFIG) {
    this.$state = $state;
    this.myTrackerApi = myTrackerApi;
    this.$scope = $scope;
    this.enableTrackerLoading = false;
    this.myAlertOnValium = myAlertOnValium;
    this.$q = $q;
    this.UI_CONFIG = UI_CONFIG;

    this.trackerServiceRunning = false;
    this.auditFlowRunning = false;
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
          return c.name === 'kafka.server.num.partitions';
        })[0].value;

        let topic = res.filter( (c) => {
          return c.name === 'audit.kafka.topic';
        })[0].value;

        let config = {
          artifact: this.UI_CONFIG.tracker.artifact,
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
          }, (err) => {
            this.myAlertOnValium.show({
              type: 'danger',
              content: err.data
            });
            this.enableTrackerLoading = false;
          });
      });

  }

  startPrograms() {
    let trackerServiceParams = {
      namespace: this.$state.params.namespace,
      programType: 'services',
      programId: this.UI_CONFIG.tracker.serviceId,
      scope: this.$scope
    };

    let auditFlowParams = {
      namespace: this.$state.params.namespace,
      programType: 'flows',
      programId: this.UI_CONFIG.tracker.flowProgramId,
      scope: this.$scope
    };

    this.myTrackerApi.startTrackerProgram(trackerServiceParams, {})
      .$promise
      .then( () => {
        this.trackerServiceRunning = true;
        this.onSuccessStartingPrograms();
      }, (err) => {
        if (err.statusCode === 409) {
          this.trackerServiceRunning = true;
          this.onSuccessStartingPrograms();
          return;
        }

        this.onErrorStartingPrograms(err);
      });

    this.myTrackerApi.startTrackerProgram(auditFlowParams, {})
      .$promise
      .then( () => {
        this.auditFlowRunning = true;
        this.onSuccessStartingPrograms();
      }, (err) => {
        if (err.statusCode === 409) {
          this.auditFlowRunning = true;
          this.onSuccessStartingPrograms();
          return;
        }

        this.onErrorStartingPrograms(err);
      });

  }

  onSuccessStartingPrograms() {
    if (!(this.auditFlowRunning && this.trackerServiceRunning)) {
      return;
    }

    this.$state.go('tracker.home');
    this.enableTrackerLoading = false;
  }

  onErrorStartingPrograms(err) {
    this.myAlertOnValium.show({
      type: 'danger',
      content: err.data
    });
    this.enableTrackerLoading = false;
  }
}

TrackerEnableController.$inject = ['$state', 'myTrackerApi', '$scope', 'myAlertOnValium', '$q', 'UI_CONFIG'];

angular.module(PKG.name + '.feature.tracker')
  .controller('TrackerEnableController', TrackerEnableController);

