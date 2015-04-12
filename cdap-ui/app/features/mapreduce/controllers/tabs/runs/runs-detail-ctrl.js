angular.module(PKG.name + '.feature.mapreduce')
  .controller('MapreduceRunsDetailController', function($scope, MyDataSource, $state) {
    var dataSrc = new MyDataSource($scope);

    $scope.tabs = [{
      title: 'Status',
      template: '/assets/features/mapreduce/templates/tabs/runs/tabs/status.html'
    },
    {
      title: 'Logs',
      template: '/assets/features/mapreduce/templates/tabs/runs/tabs/log.html'
    }];

    dataSrc.poll({
      _cdapNsPath: '/apps/' + $state.params.appId
                    + '/mapreduce/' + $state.params.programId
                    + '/runs/' + $state.params.runid + '/info'
    }, function (res) {

      $scope.info = res;
      $scope.mapProgress = Math.floor(res.mapProgress * 100);
      $scope.reduceProgress = Math.floor(res.reduceProgress * 100);

      $scope.mapperStats = getStats($scope.info.mapTasks);
      $scope.reducerStats = getStats($scope.info.reduceTasks);
    });


    $scope.getCompletedPercentage = function(tasks) {
      var aggregate = 0;
      angular.forEach(tasks, function (task) {
        if(task.state === 'SUCCEEDED') {
          aggregate += task.progress;
        }
      });
      return aggregate ? ((aggregate / (tasks.length)) * 100).toFixed(1) : 0;
    };

    $scope.getFailedTasks = function (tasks) {
      var failed = 0;
      angular.forEach(tasks, function (task) {
        if(task.state === 'FAILED') {
          failed++;
        }
      });
      return failed;
    };


    function getStats(tasks) {
      var stats = {
        completed: 0,
        running: 0,
        pending: 0,
        killed: 0,
        failed: 0,
        recordsIn: 0,
        bytesIn: 0,
        total: 0
      };

      angular.forEach(tasks, function (task) {
        if (task.state === 'SUCCEEDED') {
          stats.completed++;
        } else if (task.state === 'FAILED') {
          stats.failed++;
        } else if (task.state === 'PENDING') {
          stats.pending++;
        } else if (task.state === 'RUNNING') {
          stats.running++;
        } else if (task.state === 'KILLED') {
          stats.killed++;
        }
        stats.total++;
      });

      return stats;

    }

  });
