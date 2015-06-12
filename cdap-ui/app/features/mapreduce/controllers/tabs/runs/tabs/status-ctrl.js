angular.module(PKG.name + '.feature.mapreduce')
  .controller('MapreduceStatusController', function ($filter, $scope, $state, myMapreduceApi) {
    var myNumber = $filter('myNumber');
    if ($scope.RunsController.runs.length > 0) {
      var runid = $scope.RunsController.runs.selected.runid;

      var params = {
        namespace: $state.params.namespace,
        appId: $state.params.appId,
        mapreduceId: $state.params.programId,
        runId: runid,
        scope: $scope
      };
      myMapreduceApi.pollInfo(params)
        .$promise
        .then(function (res) {
          this.info = res;

          this.mapProgress = Math.floor(res.mapProgress * 100);
          this.reduceProgress = Math.floor(res.reduceProgress * 100);

          this.mapperStats = getStats(this.info.mapTasks, this.info.complete);
          this.mapperStats.inputRecords = myNumber(this.info.counters.MAP_INPUT_RECORDS);
          this.mapperStats.outputRecords = myNumber(this.info.counters.MAP_OUTPUT_RECORDS);

          this.reducerStats = getStats(this.info.reduceTasks, this.info.complete);
          this.reducerStats.inputRecords = myNumber(this.info.counters.REDUCE_INPUT_RECORDS);
          this.reducerStats.outputRecords = myNumber(this.info.counters.REDUCE_OUTPUT_RECORDS);
        }.bind(this));
    }

    this.getFailedTasks = function (tasks) {
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
