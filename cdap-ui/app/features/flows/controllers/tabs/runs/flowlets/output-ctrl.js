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

angular.module(PKG.name + '.feature.flows')
  .controller('FlowletDetailOutputController', function($state, $scope, MyDataSource, MyMetricsQueryHelper, MyChartHelpers, myFlowsApi) {

    var dataSrc = new MyDataSource($scope);
    var flowletid = $scope.FlowletsController.activeFlowlet.name;
    var runid = $scope.RunsController.runs.selected.runid;
    this.outputs = [];

    this.chartSettings = {
      chartMetadata: {
        showx: true,
        showy: true,
        legend: {
          show: false,
          position: 'inset'
        }
      },
      color: {
        pattern: ['red']
      },
      isLive: true,
      interval: 1000,
      aggregate: 5
    };

    var metric = {
      startTime: 'now-90s',
      endTime: 'now',
      resolution: '1s',
      names: ['system.process.events.out']
    };

    var params = {
      namespace: $state.params.namespace,
      appId: $state.params.appId,
      flowId: $state.params.programId,
      scope: $scope
    };

    var flowletTags = {
      namespace: $state.params.namespace,
      app: $state.params.appId,
      flow: $state.params.programId,
      run: runid,
      flowlet: flowletid
    };

    myFlowsApi.get(params)
      .$promise
      .then(function (res) {

        // OUTPUTS
        angular.forEach(res.connections, function(v) {
          if (v.sourceName === flowletid) {
            this.outputs.push(v.targetName);
          }
        }.bind(this));

        if (this.outputs.length > 0) {
          dataSrc
            .poll({
              _cdapPath: '/metrics/query',
              method: 'POST',
              interval: 5000,
              body: MyMetricsQueryHelper.constructQuery('qid', flowletTags, metric)
            }, function (res) {
              var processedData = MyChartHelpers.processData(
                res,
                'qid',
                metric.names,
                metric.resolution
              );

              processedData = MyChartHelpers.c3ifyData(processedData, metric, metric.names);
              this.chartData = {
                x: 'x',
                columns: processedData.columns,
                keys: {
                  x: 'x'
                }
              };
            }.bind(this));

          // Total
          dataSrc
            .poll({
              _cdapPath: '/metrics/query?' + MyMetricsQueryHelper.tagsToParams(flowletTags) + '&metric=system.process.events.out',
              method: 'POST'
            }, function(res) {
              if (res.series[0]) {
                this.total = res.series[0].data[0].value;
              }
            }.bind(this));

        }

      }.bind(this));

  });
