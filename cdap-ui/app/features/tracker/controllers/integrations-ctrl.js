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
  constructor($state, myTrackerApi, $scope, myAlertOnValium, MyCDAPDataSource) {
    this.$state = $state;
    this.myTrackerApi = myTrackerApi;
    this.$scope = $scope;
    this.myAlertOnValium = myAlertOnValium;

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

    this.navigatorState = {};

    this.getNavigatorApp();

  }

  showConfigPopup(event) {
    event.stopPropagation();
    this.showConfig = true;

    let hideConfigPopup = () => {
      this.showConfig = false;
      document.removeEventListener('click', hideConfigPopup);
    };
    document.addEventListener('click', hideConfigPopup);
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
          }
        } else {
          this.navigatorState = {
            status: 'DISABLED'
          };
        }
      });
  }

  toggleNavigator() {
    let params = {
      namespace: this.$state.params.namespace,
      action: this.navigatorSetup.isEnabled ? 'start' : 'stop',
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
    let path = '/metrics/query?tag=namespace:' + this.$state.params.namespace +
      '&tag=app:ClouderaNavigator&tag=flow:MetadataFlow&metric=system.process.events.processed&startTime=now-1h&endTime=now';
    this.dataSrc.poll({
      _cdapPath: path,
      method: 'POST'
    }, (res) => {
      console.log('res', res);
    });
  }

  saveNavigatorSetup() {
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
        this.getNavigatorApp();
      }, (err) => {
        this.myAlertOnValium.show({
          type: 'danger',
          content: err.data
        });
      });
  }

}

TrackerIntegrationsController.$inject = ['$state', 'myTrackerApi', '$scope', 'myAlertOnValium', 'MyCDAPDataSource'];

angular.module(PKG.name + '.feature.tracker')
  .controller('TrackerIntegrationsController', TrackerIntegrationsController);
