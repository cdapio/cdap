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
  .controller('HydratorDetailTopPanelController', function(DetailRunsStore, DetailNonRunsStore, PipelineDetailActionFactory, rPipelineDetail, GLOBALS) {
    this.GLOBALS = GLOBALS;
    this.app = {
      name: rPipelineDetail.name,
      description: rPipelineDetail.description,
      type: rPipelineDetail.artifact.name
    };

    this.setAppStatus = function() {
      this.appStatus = DetailRunsStore.getStatus();
      var appType = DetailRunsStore.getAppType();
      if (this.appStatus !== 'RUNNING' && appType === GLOBALS.etlBatch) {
        PipelineDetailActionFactory.fetchScheduleStatus(
          DetailRunsStore.getApi(),
          DetailRunsStore.getScheduleParams()
        );
      }
    };
    this.setScheduleStatus = function() {
      this.scheduleStatus = DetailNonRunsStore.getScheduleStatus();
      this.appStatus = this.scheduleStatus;
    };
    this.setAppStatus();
    this.setScheduleStatus();
    this.do = function(action) {
      switch(action) {
        case 'Start':
          this.appStatus = 'STARTING';
          if (this.app.type === GLOBALS.etlBatch) {
            PipelineDetailActionFactory.schedulePipeline(
              DetailRunsStore.getApi(),
              DetailRunsStore.getScheduleParams()
            );
          } else if (this.app.type === GLOBALS.etlRealtime) {
            PipelineDetailActionFactory.startPipeline(
              DetailRunsStore.getApi(),
              DetailRunsStore.getParams()
            );
          }
          break;
        case 'Run Once':
          this.appStatus = 'STARTING';
          PipelineDetailActionFactory.startPipeline(
            DetailRunsStore.getApi(),
            DetailRunsStore.getParams()
          );
          break;
        case 'Stop Schedule':
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
      }
    };
    DetailRunsStore.registerOnChangeListener(this.setAppStatus.bind(this));
    DetailNonRunsStore.registerOnChangeListener(this.setScheduleStatus.bind(this));
  });
