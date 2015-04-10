angular.module(PKG.name + '.feature.mapreduce')
  .controller('MapreduceRunsDetailController', function($scope, MyDataSource, $state) {
    $scope.tabs = [{
      title: 'Status',
      template: '/assets/features/mapreduce/templates/tabs/runs/tabs/status.html'
    },
    {
      title: 'Logs',
      template: '/assets/features/mapreduce/templates/tabs/runs/tabs/log.html'
    }];
    $scope.info = {"state":"SUCCEEDED","startTime":1428614017287,"finishTime":1428614120576,"mapProgress":0.0,"reduceProgress":0.0,"counters":{"MAP_OUTPUT_MATERIALIZED_BYTES":9644,"MAP_INPUT_RECORDS":10,"MERGED_MAP_OUTPUTS":24,"REDUCE_SHUFFLE_BYTES":9644,"SPILLED_RECORDS":200,"MAP_OUTPUT_BYTES":9300,"COMMITTED_HEAP_BYTES":5590482944,"CPU_MILLISECONDS":126790,"FAILED_SHUFFLE":0,"SPLIT_RAW_BYTES":1077,"COMBINE_INPUT_RECORDS":0,"REDUCE_INPUT_RECORDS":100,"REDUCE_INPUT_GROUPS":1,"COMBINE_OUTPUT_RECORDS":0,"PHYSICAL_MEMORY_BYTES":6160240640,"REDUCE_OUTPUT_RECORDS":1,"VIRTUAL_MEMORY_BYTES":15452643328,"MAP_OUTPUT_RECORDS":100,"SHUFFLED_MAPS":24,"GC_TIME_MILLIS":10643},"mapTasks":[{"taskId":"task_1428353338239_0078_m_000000","state":"SUCCEEDED","startTime":1428614028843,"finishTime":1428614080772,"progress":1.0,"counters":{"MAP_OUTPUT_MATERIALIZED_BYTES":9518,"MAP_INPUT_RECORDS":10,"MERGED_MAP_OUTPUTS":0,"SPILLED_RECORDS":100,"MAP_OUTPUT_BYTES":9300,"COMMITTED_HEAP_BYTES":591396864,"FAILED_SHUFFLE":0,"CPU_MILLISECONDS":14060,"SPLIT_RAW_BYTES":134,"COMBINE_INPUT_RECORDS":0,"PHYSICAL_MEMORY_BYTES":689463296,"VIRTUAL_MEMORY_BYTES":1440325632,"MAP_OUTPUT_RECORDS":100,"GC_TIME_MILLIS":2520}},{"taskId":"task_1428353338239_0078_m_000006","state":"SUCCEEDED","startTime":1428614069822,"finishTime":1428614098915,"progress":1.0,"counters":{"MAP_OUTPUT_MATERIALIZED_BYTES":18,"MAP_INPUT_RECORDS":0,"MERGED_MAP_OUTPUTS":0,"SPILLED_RECORDS":0,"MAP_OUTPUT_BYTES":0,"COMMITTED_HEAP_BYTES":597164032,"FAILED_SHUFFLE":0,"CPU_MILLISECONDS":12270,"SPLIT_RAW_BYTES":137,"COMBINE_INPUT_RECORDS":0,"PHYSICAL_MEMORY_BYTES":684478464,"VIRTUAL_MEMORY_BYTES":1427386368,"MAP_OUTPUT_RECORDS":0,"GC_TIME_MILLIS":762}}],"reduceTasks":[{"taskId":"task_1428353338239_0078_r_000002","state":"SUCCEEDED","startTime":1428614098942,"finishTime":1428614120398,"progress":1.0,"counters":{"REDUCE_SHUFFLE_BYTES":9548,"MERGED_MAP_OUTPUTS":8,"SPILLED_RECORDS":100,"COMMITTED_HEAP_BYTES":420478976,"FAILED_SHUFFLE":0,"CPU_MILLISECONDS":10160,"COMBINE_INPUT_RECORDS":0,"REDUCE_INPUT_RECORDS":100,"REDUCE_INPUT_GROUPS":1,"COMBINE_OUTPUT_RECORDS":0,"PHYSICAL_MEMORY_BYTES":435228672,"REDUCE_OUTPUT_RECORDS":1,"VIRTUAL_MEMORY_BYTES":1398706176,"SHUFFLED_MAPS":8,"GC_TIME_MILLIS":143}}]};

    $scope.mapperStats = getStats($scope.info.mapTasks);
    $scope.reducerStats = getStats($scope.info.reduceTasks);

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
    };


  });
