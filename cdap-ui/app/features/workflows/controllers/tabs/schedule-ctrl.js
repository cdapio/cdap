angular.module(PKG.name + '.feature.workflows')
  .controller('WorkflowsSchedulesController', function($scope, MyDataSource, $state) {
    var dataSrc = new MyDataSource($scope);


    dataSrc.request({
      _cdapNsPath: '/apps/' + $state.params.appId +
                    '/workflows/' + $state.params.programId + '/schedules'
    })
    .then(function (res) {
      $scope.schedules = res;

      angular.forEach($scope.schedules, function(v) {
        if (v.scheduleType === 'TIME') {
          console.log('test');
          var parse = v.schedule.cronExpression.split(' ');
          v.time = {};
          v.time.min = parse[0];
          v.time.hour = parse[1];
          v.time.day = parse[2];
          v.time.month = parse[3];
          v.time.week = parse[4];
        }
        v.isOpen = false;

        dataSrc.poll({
          _cdapNsPath: '/apps/' + $state.params.appId +
                        '/schedules/' + v.schedule.name + '/status'
        }, function (response) {
          v.status = response.status;
        });
      });

    });

    $scope.suspendSchedule = function (obj) {
      dataSrc.request({
        _cdapNsPath: '/apps/' + $state.params.appId +
                      '/schedules/' + obj.schedule.name + '/suspend',
        method: 'POST'
      });
    };

    $scope.resumeSchedule = function (obj) {
      dataSrc.request({
        _cdapNsPath: '/apps/' + $state.params.appId +
                      '/schedules/' + obj.schedule.name + '/resume',
        method: 'POST'
      });
    };

  });
