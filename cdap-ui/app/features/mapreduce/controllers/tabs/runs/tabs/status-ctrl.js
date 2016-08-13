/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

angular.module(PKG.name + '.feature.mapreduce')
  .controller('MapreduceStatusController', function ($filter, $scope, $state, MyCDAPDataSource) {
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

      var dataSrc = new MyCDAPDataSource($scope);

      /**
       * Changing it back to dataSrc because sometimes the backend returns NaN and JSON.parse error
       * out when the value is NaN. The way we are fixing this is to send the query with json: false.
       * $resource returns split string when the return value is just a simple string.
       **/
      dataSrc.poll({
        _cdapNsPath: '/apps/' + params.appId + '/mapreduce/' + params.mapreduceId + '/runs/' + params.runId + '/info',
        json: false
      }, function (res) {
        res = res.replace(/\bNaN\b/g, '"NaN"');
        try {
          res = JSON.parse(res);
        } catch(e) {
          console.warn('Unable parse response for Mapreduce program: ', params.mapreduceId);
          res = {};
        }

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
