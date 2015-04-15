angular.module(PKG.name + '.feature.mapreduce')
  .controller('MapreduceRunsDetailController', function($scope, MyDataSource, $state) {
    var dataSrc = new MyDataSource($scope);

    $scope.tabs = [{
      title: 'Status',
      template: '/assets/features/mapreduce/templates/tabs/runs/tabs/status.html'
    },
    {
      title: 'Mappers',
      template: '/assets/features/mapreduce/templates/tabs/runs/tabs/mappers.html'
    },
    {
      title: 'Reducers',
      template: '/assets/features/mapreduce/templates/tabs/runs/tabs/reducers.html'
    },
    {
      title: 'Logs',
      template: '/assets/features/mapreduce/templates/tabs/runs/tabs/log.html'
    }];


    if ($state.params.runid) {

      dataSrc.poll({
        _cdapNsPath: '/apps/' + $state.params.appId
                      + '/mapreduce/' + $state.params.programId
                      + '/runs/' + $state.params.runid + '/info'
      }, function (res) {

        $scope.info = res;
        // To Be used when progress is fixed in the backend
        // $scope.mapProgress = Math.floor(res.mapProgress * 100);
        // $scope.reduceProgress = Math.floor(res.reduceProgress * 100);

        $scope.mapperStats = getStats($scope.info.mapTasks);
        $scope.reducerStats = getStats($scope.info.reduceTasks);
      });

    } else {

      dataSrc.request({
        _cdapNsPath: '/apps/' + $state.params.appId
                      + '/mapreduce/' + $state.params.programId
                      + '/runs'
      }).then(function(list) {
        if (list.length > 0) {
          var runid = list[0].runid;

          dataSrc.poll({
            _cdapNsPath: '/apps/' + $state.params.appId
                          + '/mapreduce/' + $state.params.programId
                          + '/runs/' + runid + '/info'
          }, function (res) {

            $scope.info = res;
            // To Be used when progress is fixed in the backend
            // $scope.mapProgress = Math.floor(res.mapProgress * 100);
            // $scope.reduceProgress = Math.floor(res.reduceProgress * 100);

            $scope.mapperStats = getStats($scope.info.mapTasks);
            $scope.reducerStats = getStats($scope.info.reduceTasks);
          });
        }
      });

    }

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
        switch (task.state) {
          case 'SUCCEEDED':
            stats.completed++;
            break;
          case 'FAILED':
            stats.failed++;
            break;
          case 'SCHEDULED':
            stats.pending++;
            break;
          case 'RUNNING':
            stats.running++;
            break;
          case 'KILLED':
            stats.killed++;
            break;
        }

        stats.total++;
      });

      return stats;

    }

  });
