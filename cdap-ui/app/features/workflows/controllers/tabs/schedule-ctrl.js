class ScheduleController {
  constructor($scope, myWorkFlowApi, $state, myAlert) {
    this.myWorkFlowApi = myWorkFlowApi;
    this.$state = $state;
    this.$scope = $scope;
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

            this.myWorkFlowApi.schedulesPreviousRunTime(params)
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
          this.myWorkFlowApi.pollScheduleStatus({
            namespace: this.$state.params.namespace,
            appId: this.$state.params.appId,
            scheduleId: schedule.schedule.name,
            scope: this.$scope
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
    obj.status = 'SUSPENDING';
    this.myWorkFlowApi.scheduleSuspend({
      namespace: this.$state.params.namespace,
      appId: this.$state.params.appId,
      scheduleId: obj.schedule.name,
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
      scheduleId: obj.schedule.name,
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
ScheduleController.$inject = ['$scope', 'myWorkFlowApi', '$state'];
angular.module(`${PKG.name}.feature.workflows`)
  .controller('WorkflowsSchedulesController', ScheduleController);
