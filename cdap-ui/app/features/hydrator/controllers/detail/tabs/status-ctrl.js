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
  .controller('HydratorDetailStatusController', function(DetailRunsStore, GLOBALS, PipelineDetailActionFactory, $scope, DetailNonRunsStore) {
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
      this.config = DetailNonRunsStore.getConfigJson();
    };
    this.setState();
    this.GLOBALS = GLOBALS;
    this.pipelineType = DetailNonRunsStore.getPipelineType();
    if (this.pipelineType === GLOBALS.etlBatch) {
      params = angular.copy(DetailRunsStore.getParams());
      params.scope = $scope;
      PipelineDetailActionFactory.pollStatistics(
        DetailRunsStore.getApi(),
        params
      );
    }
    DetailRunsStore.registerOnChangeListener(this.setState.bind(this));
  });
