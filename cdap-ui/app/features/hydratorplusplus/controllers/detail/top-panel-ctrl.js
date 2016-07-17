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
  constructor(HydratorPlusPlusDetailRunsStore, HydratorPlusPlusDetailNonRunsStore, HydratorPlusPlusDetailActions, GLOBALS, $state, myLoadingService, $timeout, $scope, moment, myAlertOnValium, myPipelineExportModalService) {
    this.GLOBALS = GLOBALS;
    this.myPipelineExportModalService = myPipelineExportModalService;
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
    this.runPlayer = {
      view: false,
      action: null
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
    HydratorPlusPlusDetailRunsStore.registerOnChangeListener(() => {
      this.setAppStatus();
      this.setState();
    });
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
  exportConfig() {
    let config = angular.copy(this.HydratorPlusPlusDetailNonRunsStore.getConfigJson());
    config.stages = config.stages.map( stage => ({
      name: stage.name,
      plugin: stage.plugin
    }));
    let exportConfig = this.HydratorPlusPlusDetailNonRunsStore.getCloneConfig();
    this.myPipelineExportModalService.show(config, exportConfig);
  }
  do(action) {
    let isRunTimeArguments = true; // This will be later replaced by macros we get for a pipeline.
    switch(action) {
      case 'Start':
        if (!isRunTimeArguments) {
          this.appStatus = 'STARTING';
          this.startPipeline();
        } else {
          this.runPlayer.view = true;
          this.runPlayer.action = 'STARTING';
        }
        break;
      case 'Schedule':
        if (!isRunTimeArguments) {
          this.scheduleStatus = 'SCHEDULING';
          this.schedulePipeline();
        } else {
          this.runPlayer.view = true;
          this.runPlayer.action = 'SCHEDULING';
        }
        break;
      case 'Suspend':
        this.suspendPipeline();
        break;
      case 'Stop':
        this.appStatus = 'STOPPING';
        this.stopPipeline();
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
  startPipeline() {
    this.appStatus = 'STARTING';
    this.runPlayer.view = false;
    this.runPlayer.action = null;
    this.HydratorPlusPlusDetailActions.startPipeline(
      this.HydratorPlusPlusDetailRunsStore.getApi(),
      this.HydratorPlusPlusDetailRunsStore.getParams()
    ).then(
      () => {},
      (err) => {
        this.myAlertOnValium.show({
          type: 'danger',
          title: 'Unable to start a new run',
          content: angular.isObject(err) ? err.data: err
        });
      }
    );
  }
  stopPipeline() {
    this.HydratorPlusPlusDetailActions.stopPipeline(
      this.HydratorPlusPlusDetailRunsStore.getApi(),
      this.HydratorPlusPlusDetailRunsStore.getParams()
    ).then(
      () => {},
      (err) => {
        this.myAlertOnValium.show({
          type: 'danger',
          title: 'Unable to stop the current run',
          content: angular.isObject(err) ? err.data: err
        });
      }
    );
  }
  schedulePipeline() {
    this.runPlayer.view = false;
    this.runPlayer.action = null;
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
            content: angular.isObject(err) ? err.data: err
          });
        }
      );
  }
  suspendPipeline() {
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
            content: angular.isObject(err) ? err.data: err
          });
        }
      );
  }
}

HydratorDetailTopPanelController.$inject = ['HydratorPlusPlusDetailRunsStore', 'HydratorPlusPlusDetailNonRunsStore', 'HydratorPlusPlusDetailActions', 'GLOBALS', '$state', 'myLoadingService', '$timeout', '$scope', 'moment', 'myAlertOnValium', 'myPipelineExportModalService'];
angular.module(PKG.name + '.feature.hydratorplusplus')
  .controller('HydratorDetailTopPanelController', HydratorDetailTopPanelController);
