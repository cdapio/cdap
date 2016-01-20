/*
 * Copyright © 2015 Cask Data, Inc.
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
angular.module(PKG.name + '.feature.hydrator')
  .controller('HydratorDetailTopPanelController', function(DetailRunsStore, DetailNonRunsStore, PipelineDetailActionFactory, GLOBALS, $state, $alert, myLoadingService, $timeout, $scope, moment, myAlertOnValium) {
    this.GLOBALS = GLOBALS;
    this.myAlertOnValium = myAlertOnValium;
    this.config = DetailNonRunsStore.getCloneConfig();
    this.app = {
      name: this.config.name,
      description: this.config.description,
      type: this.config.artifact.name
    };

    this.tooltipDescription = (this.app.description && this.app.description.replace(/\n/g, '<br />')) || '' ;

    var params;
    this.setState = function() {
      this.runsCount = DetailRunsStore.getRunsCount();
      var runs = DetailRunsStore.getRuns();
      var status, i;
      var lastRunDuration;
      var nextRunTime = DetailRunsStore.getNextRunTime();
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
      var averageRunTime = DetailRunsStore.getStatistics().avgRunTime;
      // We get time as seconds from backend. So multiplying it by 1000 to give moment.js in milliseconds.
      if (averageRunTime) {
        this.avgRunTime = moment.utc( averageRunTime * 1000 ).format('HH:mm:ss');
      } else {
        this.avgRunTime = 'N/A';
      }
      this.config = DetailNonRunsStore.getCloneConfig();
    };
    this.setState();

    this.pipelineType = DetailNonRunsStore.getPipelineType();
    if (this.pipelineType === GLOBALS.etlBatch) {
      params = angular.copy(DetailRunsStore.getParams());
      params.scope = $scope;
      PipelineDetailActionFactory.pollStatistics(
        DetailRunsStore.getApi(),
        params
      );
      PipelineDetailActionFactory.pollNextRunTime(
        DetailRunsStore.getApi(),
        DetailRunsStore.getParams()
      );
    }

    this.setAppStatus = function() {
      this.appStatus = DetailRunsStore.getStatus();
      this.config = DetailNonRunsStore.getCloneConfig();
    };
    this.setScheduleStatus = function() {
      this.scheduleStatus = DetailNonRunsStore.getScheduleStatus();
    };
    this.setAppStatus();
    var appType = DetailNonRunsStore.getAppType();

    if (appType === GLOBALS.etlBatch) {
      PipelineDetailActionFactory.fetchScheduleStatus(
        DetailRunsStore.getApi(),
        DetailRunsStore.getScheduleParams()
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
          PipelineDetailActionFactory.startPipeline(
            DetailRunsStore.getApi(),
            DetailRunsStore.getParams()
          ).then(
            () => {},
            (err) => {
              this.myAlertOnValium.show({
                type: 'danger',
                title: 'Unable to start a new run',
                content: angular.isObject(err)? err.data: err,
                duration: false
              });
            }
          );
          break;
        case 'Schedule':
          this.scheduleStatus = 'SCHEDULING';
          PipelineDetailActionFactory.schedulePipeline(
            DetailRunsStore.getApi(),
            DetailRunsStore.getScheduleParams()
          )
            .then(
              () => {
                PipelineDetailActionFactory.fetchScheduleStatus(
                  DetailRunsStore.getApi(),
                  DetailRunsStore.getScheduleParams()
                );
              },
              (err) => {
                this.myAlertOnValium.show({
                  type: 'danger',
                  title: 'Unable to schedule the pipeline',
                  content: angular.isObject(err)? err.data: err,
                  duration: false
                });
              }
            );
          break;
        case 'Suspend':
          this.scheduleStatus = 'SUSPENDING';
          PipelineDetailActionFactory.suspendSchedule(
            DetailRunsStore.getApi(),
            DetailRunsStore.getScheduleParams()
          )
            .then(
              () => {
                PipelineDetailActionFactory.fetchScheduleStatus(
                  DetailRunsStore.getApi(),
                  DetailRunsStore.getScheduleParams()
                );
              },
              (err) => {
                this.myAlertOnValium.show({
                  type: 'danger',
                  title: 'Unable to suspend the pipeline',
                  content: angular.isObject(err)? err.data: err,
                  duration: false
                });
              }
            );
          break;
        case 'Stop':
          this.appStatus = 'STOPPING';
          PipelineDetailActionFactory.stopPipeline(
            DetailRunsStore.getApi(),
            DetailRunsStore.getParams()
          ).then(
            () => {},
            (err) => {
              this.myAlertOnValium.show({
                type: 'danger',
                title: 'Unable to stop the current run',
                content: angular.isObject(err)? err.data: err,
                duration: false
              });
            }
          );
          break;
        case 'Delete':
          myLoadingService.showLoadingIcon();
          var params = angular.copy(DetailRunsStore.getParams());
          params = {
            namespace: params.namespace,
            pipeline: params.app
          };
          PipelineDetailActionFactory
            .deletePipeline(params)
            .then(
              function success() {
                $state.go('^.list');
                myLoadingService.hideLoadingIcon();
              },
              function error(err) {
                myLoadingService.hideLoadingIcon();
                $timeout(() => {
                  this.myAlertOnValium.show({
                    type: 'danger',
                    title: 'Unable to delete Pipeline',
                    content: err.data,
                    duration: false
                  });
                });
              }
            );
      }
    };

    DetailRunsStore.registerOnChangeListener(this.setAppStatus.bind(this));
    DetailNonRunsStore.registerOnChangeListener(this.setScheduleStatus.bind(this));
    DetailRunsStore.registerOnChangeListener(this.setState.bind(this));
  });
