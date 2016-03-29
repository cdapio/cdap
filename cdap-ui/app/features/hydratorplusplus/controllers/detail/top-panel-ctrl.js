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
angular.module(PKG.name + '.feature.hydratorplusplus')
  .controller('HydratorDetailTopPanelController', function(HydratorPlusPlusDetailRunsStore, HydratorPlusPlusDetailNonRunsStore, HydratorPlusPlusDetailActions, GLOBALS, $state, myLoadingService, $timeout, $scope, moment, myAlertOnValium) {
    this.GLOBALS = GLOBALS;
    this.myAlertOnValium = myAlertOnValium;
    this.config = HydratorPlusPlusDetailNonRunsStore.getCloneConfig();
    this.app = {
      name: this.config.name,
      description: this.config.description,
      type: this.config.artifact.name
    };

    this.tooltipDescription = (this.app.description && this.app.description.replace(/\n/g, '<br />')) || '' ;

    var params;
    this.setState = function() {
      this.runsCount = HydratorPlusPlusDetailRunsStore.getRunsCount();
      var runs = HydratorPlusPlusDetailRunsStore.getRuns();
      var status, i;
      var lastRunDuration;
      var nextRunTime = HydratorPlusPlusDetailRunsStore.getNextRunTime();
      if (nextRunTime && nextRunTime.length) {
        this.nextRunTime = nextRunTime[0].time? nextRunTime[0].time: null;
      } else {
        this.nextRunTime = 'N/A';
      }
      for (i=0 ; i<runs.length; i++) {
        status = runs[i].status;
        if (['RUNNING', 'STARTING', 'STOPPING'].indexOf(status) === -1) {
          this.lastFinished = runs[i];
          break;
        }
      }
      if (this.lastFinished) {
        lastRunDuration = this.lastFinished.end - this.lastFinished.start;
        this.lastRunTime = moment.utc(lastRunDuration * 1000).format('HH:mm:ss');
      } else {
        this.lastRunTime = 'N/A';
      }
      var averageRunTime = HydratorPlusPlusDetailRunsStore.getStatistics().avgRunTime;
      // We get time as seconds from backend. So multiplying it by 1000 to give moment.js in milliseconds.
      if (averageRunTime) {
        this.avgRunTime = moment.utc( averageRunTime * 1000 ).format('HH:mm:ss');
      } else {
        this.avgRunTime = 'N/A';
      }
      this.config = HydratorPlusPlusDetailNonRunsStore.getCloneConfig();
    };
    this.setState();

    this.pipelineType = HydratorPlusPlusDetailNonRunsStore.getPipelineType();
    if (GLOBALS.etlBatchPipelines.indexOf(this.pipelineType) !== -1) {
      params = HydratorPlusPlusDetailRunsStore.getParams();
      params.scope = $scope;
      HydratorPlusPlusDetailActions.pollStatistics(
        HydratorPlusPlusDetailRunsStore.getApi(),
        params
      );

      let nextRunTimeParams = HydratorPlusPlusDetailRunsStore.getParams();
      nextRunTimeParams.scope = $scope;
      HydratorPlusPlusDetailActions.pollNextRunTime(
        HydratorPlusPlusDetailRunsStore.getApi(),
        nextRunTimeParams
      );
    }

    this.setAppStatus = function() {
      this.appStatus = HydratorPlusPlusDetailRunsStore.getStatus();
      this.config = HydratorPlusPlusDetailNonRunsStore.getCloneConfig();
    };
    this.setScheduleStatus = function() {
      this.scheduleStatus = HydratorPlusPlusDetailNonRunsStore.getScheduleStatus();
    };
    this.setAppStatus();
    var appType = HydratorPlusPlusDetailNonRunsStore.getAppType();

    if (GLOBALS.etlBatchPipelines.indexOf(appType) !== -1) {
      HydratorPlusPlusDetailActions.fetchScheduleStatus(
        HydratorPlusPlusDetailRunsStore.getApi(),
        HydratorPlusPlusDetailRunsStore.getScheduleParams()
      );
    }

    var greenStatus = ['COMPLETED', 'RUNNING', 'SCHEDULED', 'STARTING'];
    this.isGreenStatus = function () {
      if (greenStatus.indexOf(this.appStatus) > -1) {
        return true;
      } else {
        return false;
      }
    };

    this.do = function(action) {
      switch(action) {
        case 'Start':
          this.appStatus = 'STARTING';
          HydratorPlusPlusDetailActions.startPipeline(
            HydratorPlusPlusDetailRunsStore.getApi(),
            HydratorPlusPlusDetailRunsStore.getParams()
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
          HydratorPlusPlusDetailActions.schedulePipeline(
            HydratorPlusPlusDetailRunsStore.getApi(),
            HydratorPlusPlusDetailRunsStore.getScheduleParams()
          )
            .then(
              () => {
                HydratorPlusPlusDetailActions.fetchScheduleStatus(
                  HydratorPlusPlusDetailRunsStore.getApi(),
                  HydratorPlusPlusDetailRunsStore.getScheduleParams()
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
          HydratorPlusPlusDetailActions.suspendSchedule(
            HydratorPlusPlusDetailRunsStore.getApi(),
            HydratorPlusPlusDetailRunsStore.getScheduleParams()
          )
            .then(
              () => {
                HydratorPlusPlusDetailActions.fetchScheduleStatus(
                  HydratorPlusPlusDetailRunsStore.getApi(),
                  HydratorPlusPlusDetailRunsStore.getScheduleParams()
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
          HydratorPlusPlusDetailActions.stopPipeline(
            HydratorPlusPlusDetailRunsStore.getApi(),
            HydratorPlusPlusDetailRunsStore.getParams()
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
          myLoadingService.showLoadingIcon();
          var params = HydratorPlusPlusDetailRunsStore.getParams();
          params = {
            namespace: params.namespace,
            pipeline: params.app
          };
          HydratorPlusPlusDetailActions
            .deletePipeline(params)
            .then(
              function success() {
                $state.go('hydrator.list');
                myLoadingService.hideLoadingIcon();
              },
              function error(err) {
                myLoadingService.hideLoadingIcon();
                $timeout(() => {
                  this.myAlertOnValium.show({
                    type: 'danger',
                    title: 'Unable to delete Pipeline',
                    content: err.data
                  });
                });
              }
            );
      }
    };

    HydratorPlusPlusDetailRunsStore.registerOnChangeListener(this.setAppStatus.bind(this));
    HydratorPlusPlusDetailNonRunsStore.registerOnChangeListener(this.setScheduleStatus.bind(this));
    HydratorPlusPlusDetailRunsStore.registerOnChangeListener(this.setState.bind(this));
  });
