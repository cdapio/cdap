angular.module(PKG.name + '.feature.workflows')
  .controller('WorkflowsSchedulesController', function($scope, myWorkFlowApi, $state) {
    var params = {
      appId: $state.params.appId,
      workflowId: $state.params.programId,
      scope: $scope
    };

    myWorkFlowApi.schedules(params)
      .$promise
      .then(function(res) {
        this.schedules = res;

        angular.forEach(this.schedules, function(v) {
          if (v.scheduleType === 'TIME') {
            var parse = v.schedule.cronExpression.split(' ');
            v.time = {};
            v.time.min = parse[0];
            v.time.hour = parse[1];
            v.time.day = parse[2];
            v.time.month = parse[3];
            v.time.week = parse[4];

            myWorkFlowApi.schedulesPreviousRunTime(params)
              .$promise
              .then(function(timeResult) {
                if (timeResult[0]) {
                  v.lastrun = timeResult[0].time;
                } else {
                  v.lastrun = 'NA';
                }
              });
          } else {
            v.lastrun = 'NA';
          }
          v.isOpen = false;
          myWorkFlowApi.pollScheduleStatus({
            appId: $state.params.appId,
            scheduleId: v.schedule.name,
            scope: $scope
          })
            .$promise
            .then(function(response) {
              v.status = response.status;
            });
        });

        if (this.schedules.length > 0) {
          this.schedules[0].isOpen = true;
        }
      }.bind(this));

    this.suspendSchedule = function (obj) {
      myWorkFlowApi.scheduleSuspend({
        appId: $state.params.appId,
        scheduleId: obj.schedule.name,
        scope: $scope
      }, {});
    };

    this.resumeSchedule = function (obj) {
      myWorkFlowApi.scheduleResume({
        appId: $state.params.appId,
        scheduleId: obj.schedule.name,
        scope: $scope
      }, {});
    };

  });
