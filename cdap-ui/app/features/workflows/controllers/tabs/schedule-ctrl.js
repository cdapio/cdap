'use strict';
class ScheduleController {
  constructor($scope, myWorkFlowApi, $state, myAlert) {
    let params = {
      appId: $state.params.appId,
      workflowId: $state.params.programId,
      scope: $scope
    };

    myWorkFlowApi.schedules(params)
      .$promise
      .then( (res) => {
        this.schedules = res;

        angular.forEach(this.schedules, schedule => {
          if (schedule.scheduleType === 'TIME') {
            var parse = schedule.schedule.cronExpression.split(' ');
            schedule.time = {};
            schedule.time.min = parse[0];
            schedule.time.hour = parse[1];
            schedule.time.day = parse[2];
            schedule.time.month = parse[3];
            schedule.time.week = parse[4];

            myWorkFlowApi.schedulesPreviousRunTime(params)
              .$promise
              .then( timeResult => {
                if (timeResult[0]) {
                  schedule.lastrun = timeResult[0].time;
                } else {
                  schedule.lastrun = 'NA';
                }
              });
          } else {
            schedule.lastrun = 'NA';
          }
          schedule.isOpen = false;
          myWorkFlowApi.pollScheduleStatus({
            appId: $state.params.appId,
            scheduleId: schedule.schedule.name,
            scope: $scope
          })
            .$promise
            .then( response => {
              schedule.status = response.status;
            });
        });

        if (this.schedules.length > 0) {
          this.schedules[0].isOpen = true;
        }
      });
  }

  suspendSchedule(obj) {
    myWorkFlowApi.scheduleSuspend({
      appId: $state.params.appId,
      scheduleId: obj.schedule.name,
      scope: $scope
    }, {},
    function success() {},
    function error(err) {
      myAlert({
        title: 'Cannot Suspend Schedule',
        content: err
      });
    });
  }

  resumeSchedule(obj) {
    myWorkFlowApi.scheduleResume({
      appId: $state.params.appId,
      scheduleId: obj.schedule.name,
      scope: $scope
    }, {},
    function success() {},
    function error(err) {
      myAlert({
        title: 'Cannot Resume Schedule',
        content: err
      });
    });
  }
}
ScheduleController.$inject = ['$scope', 'myWorkFlowApi', '$state']
angular.module(PKG.name + '.feature.workflows')
  .controller('WorkflowsSchedulesController', ScheduleController);
