  /*
  * Copyright © 2018 Cask Data, Inc.
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

class MyPipelineSchedulerCtrl {
  constructor() {
    this.schedule = this.store.getSchedule();
    this.maxConcurrentRuns = this.store.getMaxConcurrentRuns();
  }
}

angular.module(PKG.name + '.commons')
.controller('MyPipelineSchedulerCtrl', MyPipelineSchedulerCtrl)
.directive('myPipelineScheduler', function() {
  return {
    restrict: 'E',
    scope: {
      store: '=',
      actionCreator: '=',
      pipelineName: '@',
      onClose: '&',
      anchorEl: '@',
    },
    bindToController: true,
    controller: 'MyPipelineSchedulerCtrl',
    controllerAs: 'SchedulerCtrl',
    templateUrl: 'my-pipeline-scheduler/my-pipeline-scheduler.html'
  };
});
