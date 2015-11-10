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

class ScheduleController {
  constructor($scope, myWorkFlowApi, $state, myAlert, $q) {
    this.myWorkFlowApi = myWorkFlowApi;
    this.$state = $state;
    this.$scope = $scope;
    this.$q = $q;
    this.myAlert = myAlert;

    let params = {
      namespace: this.$state.params.namespace,
      appId: this.$state.params.appId,
      workflowId: this.$state.params.programId,
      scope: this.$scope
    };

    this.myWorkFlowApi.schedules(params)
      .$promise
      .then( (res) => {
        this.schedules = [];

        angular.forEach(res, scheduleResource => {
          var schedule = scheduleResource.schedule;
          schedule.isOpen = false;
          schedule.scheduleType = scheduleResource.scheduleType;
          this.schedules.push(schedule);
          this.getPreviousRunTime()
            .then( lastrun => {
              schedule.lastrun = lastrun;
              return this.myWorkFlowApi.pollScheduleStatus({
                namespace: this.$state.params.namespace,
                appId: this.$state.params.appId,
                scheduleId: schedule.name,
                scope: this.$scope
              })
              .$promise
              .then(status => {
                schedule.status = status.status;
              });
            });
        });
        if (this.schedules.length > 0) {
          this.schedules[0].isOpen = true;
        }
      });
  }

  getPreviousRunTime(scheduleType) {
    var defer = this.$q.defer();
    var lastrun;

    let params = {
      namespace: this.$state.params.namespace,
      appId: this.$state.params.appId,
      workflowId: this.$state.params.programId,
      scope: this.$scope
    };

    if (scheduleType === 'TIME') {
      this.myWorkFlowApi.schedulesPreviousRunTime(params)
        .$promise
        .then( timeResult => {
          if (timeResult[0]) {
            lastrun = timeResult[0].time;
          } else {
            lastrun = 'NA';
          }
          defer.resolve(lastrun);
        });
    } else {
      lastrun = 'NA';
      defer.resolve(lastrun);
    }
    return defer.promise;
  }

  scheduleActionsMediator(schedule, action) {
    switch(action) {
      case 'pause':
        this.suspendSchedule(schedule);
        break;
      case 'resume':
        this.resumeSchedule(schedule);
        break;
    }
  }

  suspendSchedule(obj) {
    obj.status = 'SUSPENDING';
    this.myWorkFlowApi.scheduleSuspend({
      namespace: this.$state.params.namespace,
      appId: this.$state.params.appId,
      scheduleId: obj.name,
      scope: this.$scope
    }, {},
    function success() {},
    function error(err) {
      this.myAlert({
        title: 'Cannot Suspend Schedule',
        content: err
      });
    }.bind(this));
  }

  resumeSchedule(obj) {
    obj.status = 'RESUMING';
    this.myWorkFlowApi.scheduleResume({
      namespace: this.$state.params.namespace,
      appId: this.$state.params.appId,
      scheduleId: obj.name,
      scope: this.$scope
    }, {},
    function success() {},
    function error(err) {
      this.myAlert({
        title: 'Cannot Resume Schedule',
        content: err
      });
    }.bind(this));
  }
}
ScheduleController.$inject = ['$scope', 'myWorkFlowApi', '$state', 'myAlert', '$q'];
angular.module(`${PKG.name}.feature.workflows`)
  .controller('WorkflowsSchedulesController', ScheduleController);
