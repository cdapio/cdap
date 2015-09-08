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
  .controller('FlowsRunDetailStatusController', function($state, $scope, MyDataSource, myHelpers, FlowDiagramData, $timeout, MyMetricsQueryHelper, myFlowsApi) {
    var dataSrc = new MyDataSource($scope);

    this.data = {};
    this.data.metrics = {};
    this.data.instances = {};
    this.onChangeFlag = 1;

    FlowDiagramData.fetchData($state.params.appId, $state.params.programId)
      .then(function(data) {
        this.data = data;
        this.data.metrics = {};
        this.data.instances = {};
        pollMetrics.bind(this)();
      }.bind(this));

    function generateStreamMetricsPath(streamName) {
      var streamTags = {
        namespace: $state.params.namespace,
        stream: streamName
      };
      return '/metrics/query?metric=system.collect.events&aggregate=true&' + MyMetricsQueryHelper.tagsToParams(streamTags);
    }

    function pollMetrics() {
      var nodes = this.data.nodes;
      // Requesting Metrics data
      // We just need to poll for streams, individually, in a flow for processed events.
      // The flowlet metrics could be batched and made as single request.
      // There is a groupBy: ['flowlet'] but not 'stream'
      nodes.forEach(function(node) {
        if (node.type === 'STREAM') {
          dataSrc.poll({
            _cdapPath: generateStreamMetricsPath(node.name),
            method: 'POST',
            interval: 2000
          }, function(data) {
            this.data.metrics[node.name] = myHelpers.objectQuery(data, 'series' , 0, 'data', 0, 'value') || 0;
          }.bind(this));
        } else {
          var params = {
            namespace: $state.params.namespace,
            appId: $state.params.appId,
            flowId: $state.params.programId,
            flowletId: node.name,
            scope: $scope
          };
          myFlowsApi.pollFlowletInstance(params)
            .$promise
            .then(function (res) {
              this.data.instances[node.name] = res.instances;
              this.onChangeFlag += 1;
            }.bind(this));
        }

      }.bind(this));

      // Batched query for flowlet metrics.
      dataSrc.poll(
        {
          _cdapPath: '/metrics/query',
          method: 'POST',
          body: MyMetricsQueryHelper.constructQuery(
            'qid',
            {
              namespace: $state.params.namespace,
              app: $state.params.appId,
              flow: $state.params.programId,
              run: $scope.RunsController.runs.selected.runid
            },
            {names: ['system.process.events.processed']},
            false,
            ['flowlet', 'stream']
          )
        }, function(res) {
          var dataSeries = res.qid.series;

          if (dataSeries.length) {
            dataSeries.forEach(function(data) {
              this.data.metrics[data.grouping.flowlet] = data.data[0].value;
            }.bind(this));
            this.onChangeFlag += 1;
          }
        }.bind(this));
    }

    this.flowletClick = function(node) {
      $scope.RunsController.selectTab($scope.RunsController.tabs[1], node);
    };

    $scope.$on('$destroy', function () {
      FlowDiagramData.reset();
    });
  });
