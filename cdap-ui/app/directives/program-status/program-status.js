angular.module(PKG.name + '.commons')
  .directive('myProgramStatus', function () {

    return {
      restrict: 'E',
      scope: {
        type: '@',
        runid: '@',
        pollInterval: '@',
        runs: '='
      },
      templateUrl: 'program-status/program-status.html',
      controller: function($scope, MyDataSource, $state) {
        var pollPromise;
        // $scope.runs comes from parent controller
        if ($scope.runs.length !== 0) {
          var dataSrc = new MyDataSource($scope),
              path = '';

          if ($scope.type === 'adapters') {
            path = '/adapters/' + $state.params.adapterId + '/runs/' + $scope.runid;
          } else {
            path = '/apps/' + $state.params.appId + '/' + $scope.type + '/' + $state.params.programId + '/runs/' + $scope.runid;
          }

          pollPromise = dataSrc.poll({
            _cdapNsPath: path,
            interval: $scope.pollInterval || 10000
          }, function (res) {
            var startMs = res.start * 1000;
            $scope.start = new Date(startMs);
            $scope.status = res.status;
            $scope.duration = (res.end ? (res.end * 1000) - startMs : 0);
            if (['COMPLETED', 'KILLED', 'STOPPED', 'FAILED'].indexOf($scope.status) !== -1) {
              dataSrc.stopPoll(pollPromise.__pollId__);
            }
          });
        }

      }
    };
  });
