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
  .controller('HydratorDetailHistoryController', function(DetailRunsStore, GLOBALS) {
    this.setState = function() {
      this.history = DetailRunsStore.getHistory();
      var appType = DetailRunsStore.getAppType();
      if (appType === GLOBALS.etlBatch) {
        this.programId = DetailRunsStore.getParams().workflowId;
        this.programType = 'WORKFLOWS';
      } else if (appType === GLOBALS.etlRealtime) {
        this.programId = DetailRunsStore.getParams().workerId;
        this.programType = 'WORKER';
      }
      this.appId = DetailRunsStore.getParams().appId;
    };
    this.setState();
    DetailRunsStore.registerOnChangeListener(this.setState.bind(this));
  });
