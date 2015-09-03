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

    FlowDiagramData.fetchData($state.params.appId, $state.params.programId)
      .then(function(data) {
        this.data = data;
        pollMetrics.bind(this)();
      }.bind(this));

    function generateStreamMetricsPath(streamName) {
      var streamTags = {
        namespace: $state.params.namespace,
        stream: streamName
      };
      return '/metrics/query?metric=system.collect.events&aggregate=true&' + MyMetricsQueryHelper.tagsToParams(streamTags);
    }

    function generateFlowletMetricsPath(flowletName) {
      var flowletTags = {
        namespace: $state.params.namespace,
        app: $state.params.appId,
        flow: $state.params.programId,
        run: $scope.RunsController.runs.selected.runid,
        flowlet: flowletName
      };
      return '/metrics/query?metric=system.process.events.processed&aggregate=true&' + MyMetricsQueryHelper.tagsToParams(flowletTags);
    }

    function pollMetrics() {
      var nodes = this.data.nodes;
      this.data.instances = {};
      // Requesting Metrics data
      angular.forEach(nodes, function (node) {
        if (node.type !== 'STREAM' && !$scope.RunsController.runs.length) {
          return;
        }
        dataSrc.poll({
          _cdapPath: (node.type === 'STREAM' ? generateStreamMetricsPath(node.name): generateFlowletMetricsPath(node.name)),
          method: 'POST',
          interval: 2000
        }, function (data) {
          this.data.metrics[node.name] = myHelpers.objectQuery(data, 'series' , 0, 'data', 0, 'value') || 0;
        }.bind(this));

        // Polling for Flowlet Instance
        if (node.type !== 'STREAM') {
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
            }.bind(this));
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
