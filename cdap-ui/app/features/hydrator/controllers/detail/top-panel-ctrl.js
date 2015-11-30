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
  .controller('HydratorDetailTopPanelController', function(DetailRunsStore, DetailNonRunsStore, PipelineDetailActionFactory, rPipelineDetail, GLOBALS, $state, $alert, myLoadingService, $timeout, $scope) {
    this.GLOBALS = GLOBALS;
    this.config = DetailRunsStore.getCloneConfig();
    this.app = {
      name: rPipelineDetail.name,
      description: rPipelineDetail.description,
      type: rPipelineDetail.artifact.name
    };

    var params;
    this.setState = function() {
      this.runsCount = DetailRunsStore.getRunsCount();
      var runs = DetailRunsStore.getRuns();
      var status, i;
      for (i=0 ; i<runs.length; i++) {
        status = runs[i].status;
        if (['RUNNING', 'STARTING', 'STOPPING'].indexOf(status) === -1) {
          this.lastFinished = runs[i];
          break;
        }
      }
      this.lastRunTime = runs.length > 0 && runs[0].end ? runs[0].end - runs[0].start : 'N/A';
      this.averageRunTime = DetailRunsStore.getStatistics().avgRunTime || 'N/A';
      this.config = DetailRunsStore.getConfigJson();
    };
    this.setState();

    this.pipelineType = DetailRunsStore.getPipelineType();
    if (this.pipelineType === GLOBALS.etlBatch) {
      params = angular.copy(DetailRunsStore.getParams());
      params.scope = $scope;
      PipelineDetailActionFactory.pollStatistics(
        DetailRunsStore.getApi(),
        params
      );
    }

    this.setAppStatus = function() {
      this.appStatus = DetailRunsStore.getStatus();
      this.config = DetailRunsStore.getCloneConfig();
    };
    this.setScheduleStatus = function() {
      this.scheduleStatus = DetailNonRunsStore.getScheduleStatus();
    };
    this.setAppStatus();
    var appType = DetailRunsStore.getAppType();

    if (appType === GLOBALS.etlBatch) {
      PipelineDetailActionFactory.fetchScheduleStatus(
        DetailRunsStore.getApi(),
        DetailRunsStore.getScheduleParams()
      );
    }
    this.do = function(action) {
      switch(action) {
        case 'Start':
          this.appStatus = 'STARTING';
          PipelineDetailActionFactory.startPipeline(
            DetailRunsStore.getApi(),
            DetailRunsStore.getParams()
          );
          break;
        case 'Schedule':
          this.scheduleStatus = 'SCHEDULING';
          PipelineDetailActionFactory.schedulePipeline(
            DetailRunsStore.getApi(),
            DetailRunsStore.getScheduleParams()
          )
            .then(function() {
              PipelineDetailActionFactory.fetchScheduleStatus(
                DetailRunsStore.getApi(),
                DetailRunsStore.getScheduleParams()
              );
            });
          break;
        case 'Suspend':
          this.scheduleStatus = 'SUSPENDING';
          PipelineDetailActionFactory.suspendSchedule(
            DetailRunsStore.getApi(),
            DetailRunsStore.getScheduleParams()
          )
            .then(function() {
              PipelineDetailActionFactory.fetchScheduleStatus(
                DetailRunsStore.getApi(),
                DetailRunsStore.getScheduleParams()
              );
            });
          break;
        case 'Stop':
          this.appStatus = 'STOPPING';
          PipelineDetailActionFactory.stopPipeline(
            DetailRunsStore.getApi(),
            DetailRunsStore.getParams()
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
                $timeout(function() {
                  $alert({
                    type: 'danger',
                    title: 'Unable to delete Pipeline',
                    content: err.data
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
