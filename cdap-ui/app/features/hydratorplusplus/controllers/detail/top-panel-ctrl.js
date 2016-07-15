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

class HydratorDetailTopPanelController {
  constructor(HydratorPlusPlusDetailRunsStore, HydratorPlusPlusDetailNonRunsStore, HydratorPlusPlusDetailActions, GLOBALS, $state, myLoadingService, $timeout, $scope, moment, myAlertOnValium) {
    this.GLOBALS = GLOBALS;
    this.myAlertOnValium = myAlertOnValium;
    this.$state = $state;
    this.moment = moment;
    this.$timeout = $timeout;
    this.HydratorPlusPlusDetailNonRunsStore = HydratorPlusPlusDetailNonRunsStore;
    this.HydratorPlusPlusDetailRunsStore = HydratorPlusPlusDetailRunsStore;
    this.HydratorPlusPlusDetailActions = HydratorPlusPlusDetailActions;
    this.config = HydratorPlusPlusDetailNonRunsStore.getCloneConfig();
    this.app = {
      name: this.config.name,
      description: this.config.description,
      type: this.config.artifact.name
    };
    this.pipelineType = HydratorPlusPlusDetailNonRunsStore.getPipelineType();
    this.myLoadingService = myLoadingService;
    this.tooltipDescription = (this.app.description && this.app.description.replace(/\n/g, '<br />')) || '' ;
    this.viewSettings = false;
    this.setState();
    this.setAppStatus();
    var appType = HydratorPlusPlusDetailNonRunsStore.getAppType();
    if (GLOBALS.etlBatchPipelines.indexOf(appType) !== -1) {
      HydratorPlusPlusDetailActions.fetchScheduleStatus(
        HydratorPlusPlusDetailRunsStore.getApi(),
        HydratorPlusPlusDetailRunsStore.getScheduleParams()
      );
    }
    HydratorPlusPlusDetailRunsStore.registerOnChangeListener(this.setAppStatus.bind(this));
    HydratorPlusPlusDetailRunsStore.registerOnChangeListener(this.setState.bind(this));
    HydratorPlusPlusDetailNonRunsStore.registerOnChangeListener(this.setScheduleStatus.bind(this));
  }
  setState() {
    var runs = this.HydratorPlusPlusDetailRunsStore.getRuns();
    var status, i;
    var lastRunDuration;
    for (i=0 ; i<runs.length; i++) {
      status = runs[i].status;
      if (['RUNNING', 'STARTING', 'STOPPING'].indexOf(status) === -1) {
        this.lastFinished = runs[i];
        break;
      }
    }
    if (this.lastFinished) {
      lastRunDuration = this.lastFinished.end - this.lastFinished.start;
      this.lastRunTime = this.moment.utc(lastRunDuration * 1000).format('HH:mm:ss');
    } else {
      this.lastRunTime = 'N/A';
    }
    this.config = this.HydratorPlusPlusDetailNonRunsStore.getCloneConfig();
  }
  setAppStatus() {
    this.appStatus = this.HydratorPlusPlusDetailRunsStore.getStatus();
    this.config = this.HydratorPlusPlusDetailNonRunsStore.getCloneConfig();
  }
  setScheduleStatus() {
    this.scheduleStatus = this.HydratorPlusPlusDetailNonRunsStore.getScheduleStatus();
  }
  isGreenStatus() {
    var greenStatus = ['COMPLETED', 'RUNNING', 'SCHEDULED', 'STARTING'];
    if (greenStatus.indexOf(this.appStatus) > -1) {
      return true;
    } else {
      return false;
    }
  }
  do(action) {
    switch(action) {
      case 'Start':
        this.appStatus = 'STARTING';
        this.HydratorPlusPlusDetailActions.startPipeline(
          this.HydratorPlusPlusDetailRunsStore.getApi(),
          this.HydratorPlusPlusDetailRunsStore.getParams()
        ).then(
          () => {},
          (err) => {
            this.myAlertOnValium.show({
              type: 'danger',
              title: 'Unable to start a new run',
              content: angular.isObject(err)? err.data: err
            });
          }
        );
        break;
      case 'Schedule':
        this.scheduleStatus = 'SCHEDULING';
        this.HydratorPlusPlusDetailActions.schedulePipeline(
          this.HydratorPlusPlusDetailRunsStore.getApi(),
          this.HydratorPlusPlusDetailRunsStore.getScheduleParams()
        )
          .then(
            () => {
              this.HydratorPlusPlusDetailActions.fetchScheduleStatus(
                this.HydratorPlusPlusDetailRunsStore.getApi(),
                this.HydratorPlusPlusDetailRunsStore.getScheduleParams()
              );
            },
            (err) => {
              this.myAlertOnValium.show({
                type: 'danger',
                title: 'Unable to schedule the pipeline',
                content: angular.isObject(err)? err.data: err
              });
            }
          );
        break;
      case 'Suspend':
        this.scheduleStatus = 'SUSPENDING';
        this.HydratorPlusPlusDetailActions.suspendSchedule(
          this.HydratorPlusPlusDetailRunsStore.getApi(),
          this.HydratorPlusPlusDetailRunsStore.getScheduleParams()
        )
          .then(
            () => {
              this.HydratorPlusPlusDetailActions.fetchScheduleStatus(
                this.HydratorPlusPlusDetailRunsStore.getApi(),
                this.HydratorPlusPlusDetailRunsStore.getScheduleParams()
              );
            },
            (err) => {
              this.myAlertOnValium.show({
                type: 'danger',
                title: 'Unable to suspend the pipeline',
                content: angular.isObject(err)? err.data: err
              });
            }
          );
        break;
      case 'Stop':
        this.appStatus = 'STOPPING';
        this.HydratorPlusPlusDetailActions.stopPipeline(
          this.HydratorPlusPlusDetailRunsStore.getApi(),
          this.HydratorPlusPlusDetailRunsStore.getParams()
        ).then(
          () => {},
          (err) => {
            this.myAlertOnValium.show({
              type: 'danger',
              title: 'Unable to stop the current run',
              content: angular.isObject(err)? err.data: err
            });
          }
        );
        break;
      case 'Delete':
        this.myLoadingService.showLoadingIcon();
        var params = this.HydratorPlusPlusDetailRunsStore.getParams();
        params = {
          namespace: params.namespace,
          pipeline: params.app
        };
        this.HydratorPlusPlusDetailActions
          .deletePipeline(params)
          .then(
            function success() {
              this.$state.go('hydratorplusplus.list');
              this.myLoadingService.hideLoadingIcon();
            },
            function error(err) {
              this.myLoadingService.hideLoadingIcon();
              this.$timeout(() => {
                this.myAlertOnValium.show({
                  type: 'danger',
                  title: 'Unable to delete Pipeline',
                  content: err.data
                });
              });
            }
          );
    }
  }
}

HydratorDetailTopPanelController.$inject = ['HydratorPlusPlusDetailRunsStore', 'HydratorPlusPlusDetailNonRunsStore', 'HydratorPlusPlusDetailActions', 'GLOBALS', '$state', 'myLoadingService', '$timeout', '$scope', 'moment', 'myAlertOnValium'];
angular.module(PKG.name + '.feature.hydratorplusplus')
  .controller('HydratorDetailTopPanelController', HydratorDetailTopPanelController);
