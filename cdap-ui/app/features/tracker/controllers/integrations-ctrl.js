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

class TrackerIntegrationsController {
  constructor($state, myTrackerApi, $scope, myAlertOnValium, MyCDAPDataSource, MyChartHelpers, MyMetricsQueryHelper, $uibModal) {
    this.$state = $state;
    this.myTrackerApi = myTrackerApi;
    this.$scope = $scope;
    this.myAlertOnValium = myAlertOnValium;
    this.MyChartHelpers = MyChartHelpers;
    this.MyMetricsQueryHelper = MyMetricsQueryHelper;
    this.$uibModal = $uibModal;

    this.dataSrc = new MyCDAPDataSource($scope);

    this.navigatorSetup = {
      isOpen: false,
      isSetup: false,
      isEnabled: false
    };
    this.showConfig = false;

    this.navigatorInfo = {
      brokerString: '',
      hostname: '',
      username: '',
      password: ''
    };

    this.chartSettings = {
      chartMetadata: {
        showx: false,
        showy: false,
        legend: {
          show: false,
          position: 'inset'
        }
      },
      color: {
        pattern: ['#35c853'] // @tracker-green
      },
      isLive: true,
      interval: 1000,
      aggregate: 5
    };

    this.navigatorState = {};

    this.getNavigatorApp();

    this.pollId = null;

  }

  getKafkaBrokerList() {
    this.myTrackerApi.getCDAPConfig({ scope: this.$scope })
      .$promise
      .then( (res) => {
        let filtered = res.filter( (config) => {
          return config.name === 'metadata.updates.kafka.broker.list';
        });
        this.navigatorInfo.brokerString = filtered[0].value;
      });
  }

  getNavigatorApp() {
    let params = {
      namespace: this.$state.params.namespace,
      scope: this.$scope
    };
    this.myTrackerApi.getNavigatorApp(params)
      .$promise
      .then((res) => {
        this.navigatorSetup.isSetup = true;
        let config = {};
        try {
          config = JSON.parse(res.configuration);

          this.navigatorInfo.hostname = config.navigatorConfig.navigatorHostName;
          this.navigatorInfo.username = config.navigatorConfig.username;
          this.navigatorInfo.password = config.navigatorConfig.password;
          this.navigatorInfo.brokerString = config.metadataKafkaConfig.brokerString;

          this.getNavigatorStatus();
        } catch (e) {
          console.log('Cannot parse configuration JSON');
        }
      }, () => {
        this.getKafkaBrokerList();
      });
  }

  getNavigatorStatus() {
    let params = {
      namespace: this.$state.params.namespace,
      scope: this.$scope
    };

    this.myTrackerApi.getNavigatorStatus(params)
      .$promise
      .then((res) => {
        if (res.length) {
          this.navigatorState = res[0];

          if (res[0].status === 'RUNNING') {
            this.navigatorSetup.isEnabled = true;
            this.fetchMetrics();

            this.logsParams = {
              namespace: this.$state.params.namespace,
              appId: '_ClouderaNavigator',
              programType: 'flows',
              programId: 'MetadataFlow',
              runId: res[0].runid
            };
          } else {
            this.dataSrc.stopPoll(this.pollId.__pollId__);
          }
        } else {
          this.navigatorState = {
            status: 'DISABLED'
          };
        }
      });
  }

  toggleNavigator() {
    if (this.navigatorSetup.isEnabled) {
      let modal = this.$uibModal.open({
        templateUrl: '/assets/features/tracker/templates/partial/navigator-confirm-modal.html',
        size: 'md',
        windowClass: 'navigator-confirm-modal'
      });

      modal.result.then((check) => {
        if (check === 'disable') {
          this.navigatorSetup.isEnabled = false;
          this.navigatorState.status = 'STOPPING';
          this.navigatorFlowAction('stop');
        }
      });
    } else {
      this.navigatorState.status = 'STARTING';
      this.navigatorFlowAction('start');
      this.navigatorSetup.isEnabled = true;
    }
  }

  navigatorFlowAction(action) {
    let params = {
      namespace: this.$state.params.namespace,
      action: action,
      scope: this.$scope
    };

    this.myTrackerApi.toggleNavigator(params, {})
      .$promise
      .then( () => {
        this.getNavigatorStatus();
      }, (err) => {
        this.myAlertOnValium.show({
          type: 'danger',
          content: err.data
        });
      });
  }

  fetchMetrics() {

    let metric = {
      startTime: 'now-1h',
      endTime: 'now',
      resolution: '1m',
      names: ['system.process.events.processed']
    };

    let tags = {
      namespace: this.$state.params.namespace,
      app: '_ClouderaNavigator',
      flow: 'MetadataFlow'
    };

    // fetch timeseries metrics
    this.pollId = this.dataSrc.poll({
      _cdapPath: '/metrics/query',
      method: 'POST',
      body: this.MyMetricsQueryHelper.constructQuery('qid', tags, metric)
    }, (res) => {
      let processedData = this.MyChartHelpers.processData(
        res,
        'qid',
        metric.names,
        metric.resolution
      );

      processedData = this.MyChartHelpers.c3ifyData(processedData, metric, metric.names);
      this.chartData = {
        x: 'x',
        columns: processedData.columns,
        keys: {
          x: 'x'
        }
      };

      this.eventsSentAggregate = _.sum(this.chartData.columns[0]);
    });
  }

  saveNavigatorSetup() {
    this.saving = true;
    let params = {
      namespace: this.$state.params.namespace,
      scope: this.$scope
    };

    let config = {
      artifact: {
        name: 'navigator',
        version: '0.2.0-SNAPSHOT',
        scope: 'USER'
      },
      config: {
        navigatorConfig: {
          navigatorHostName: this.navigatorInfo.hostname,
          username: this.navigatorInfo.username,
          password: this.navigatorInfo.password
        },
        metadataKafkaConfig: {
          brokerString: this.navigatorInfo.brokerString
        }
      }
    };

    this.myTrackerApi.deployNavigator(params, config)
      .$promise
      .then( () => {
        this.navigatorSetup.isOpen = false;
        this.saving = false;
        this.getNavigatorApp();
      }, (err) => {
        this.saving = false;
        this.myAlertOnValium.show({
          type: 'danger',
          content: err.data
        });
      });
  }

}

TrackerIntegrationsController.$inject = ['$state', 'myTrackerApi', '$scope', 'myAlertOnValium', 'MyCDAPDataSource', 'MyChartHelpers', 'MyMetricsQueryHelper', '$uibModal'];

angular.module(PKG.name + '.feature.tracker')
  .controller('TrackerIntegrationsController', TrackerIntegrationsController);
