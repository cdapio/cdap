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
  .service('PipelineDetailActionFactory', function(PipelineDetailDispatcher) {
    var dispatcher = PipelineDetailDispatcher.getDispatcher();

    this.startPipeline = function (api, params) {
      api.doAction(angular.extend(params, { action: 'start' }), {});
    };
    this.schedulePipeline = function(api, scheduleParams) {
      api.scheduleResume(scheduleParams, {});
    };

    this.stopPipeline = function (api, params) {
      api.doAction(angular.extend(params, { action: 'stop' }), {});
    };
    this.suspendSchedule = function(api, params) {
      api.scheduleSuspend(params, {});
    };

    this.pollRuns = function(api, params) {
      api.pollRuns(params)
        .$promise
        .then(function(runs) {
          dispatcher.dispatch('onRunsChange', runs);
        });
    };
    this.fetchScheduleStatus = function(api, params) {
      api.getScheduleStatus(params)
        .$promise
        .then(function(res) {
          dispatcher.dispatch('onScheduleStatusFetch', res);
        });
    };
    this.pollStatistics = function(api, params) {
      api.pollStatistics(params)
          .$promise
          .then(function(res) {
            dispatcher.dispatch('onStatisticsFetch', res);
          });
    };
  });
