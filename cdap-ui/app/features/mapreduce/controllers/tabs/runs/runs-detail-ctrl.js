angular.module(PKG.name + '.feature.mapreduce')
  .controller('MapreduceRunsDetailController', function($scope, MyDataSource, $state, $filter) {
    var dataSrc = new MyDataSource($scope);
    var myNumber = $filter('myNumber');

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

    $scope.activeTab = $scope.tabs[0];

    $scope.selectTab = function(tab) {
      $scope.activeTab = tab;
    };

    if ($scope.current !== 'No Run') {
      var runid = $scope.current;

      dataSrc.poll({
        _cdapNsPath: '/apps/' + $state.params.appId
                      + '/mapreduce/' + $state.params.programId
                      + '/runs/' + runid + '/info'
      }, function (res) {

        $scope.info = res;
        $scope.mapProgress = Math.floor(res.mapProgress * 100);
        $scope.reduceProgress = Math.floor(res.reduceProgress * 100);

        $scope.mapperStats = getStats($scope.info.mapTasks, $scope.info.complete);
        $scope.mapperStats.inputRecords = myNumber($scope.info.counters.MAP_INPUT_RECORDS);
        $scope.mapperStats.outputRecords = myNumber($scope.info.counters.MAP_OUTPUT_RECORDS);

        $scope.reducerStats = getStats($scope.info.reduceTasks, $scope.info.complete);
        $scope.reducerStats.inputRecords = myNumber($scope.info.counters.REDUCE_INPUT_RECORDS);
        $scope.reducerStats.outputRecords = myNumber($scope.info.counters.REDUCE_OUTPUT_RECORDS);
      });
    }

    $scope.getFailedTasks = function (tasks) {
      var failed = 0;
      angular.forEach(tasks, function (task) {
        if(task.state === 'FAILED') {
          failed++;
        }
      });
      return failed;
    };


    function getStats(tasks, completeInfo) {
      var stats = {
        completed: 0,
        running: 0,
        pending: 0,
        killed: 0,
        failed: 0,
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

      if (completeInfo === false) {
        var NA = 'NA';
        return {
          completed: NA,
          running: NA,
          pending: NA,
          killed: NA,
          failed: NA,
          total: stats.total
        };
      }

      angular.forEach(Object.keys(stats), function(key) {
        stats[key] = myNumber(stats[key]);
      });
      return stats;

    }

  });
