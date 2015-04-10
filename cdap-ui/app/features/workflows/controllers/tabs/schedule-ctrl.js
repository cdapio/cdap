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
      });

      $scope.toggleLine = function (schedule) {
        schedule.isOpen = !schedule.isOpen;
      };
    });
    // // TODO: change to real schedule
    // $scope.schedules = [
    //   {
    //     'program': {
    //       'programName': 'PurchaseHistoryWorkflow',
    //       'programType': 'WORKFLOW'
    //     },
    //     'properties': {},
    //     'schedule': {
    //       'cronExpression': '0 4 * * *',
    //       'description': 'Schedule execution every day',
    //       'name': 'DailySchedule'
    //     },
    //     'scheduleType': 'TIME'
    //   },
    //   {
    //     'program': {
    //       'programName': 'PurchaseHistoryWorkflow',
    //       'programType': 'WORKFLOW'
    //     },
    //     'properties': {},
    //     'schedule': {
    //       'dataTriggerMB': 1,
    //       'description': 'Schedule execution when 1 MB or more of data is ingested in the   purchaseStream',
    //       'name': 'DataSchedule',
    //       'streamName': 'purchaseStream'
    //     },
    //     'scheduleType': 'STREAM'
    //   }
    // ];




  });
