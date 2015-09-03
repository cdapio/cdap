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

angular.module(PKG.name + '.feature.adapters')
  // TODO: We should use rAdapterDetail here since this data is already resolved at adapter.detail state
  .controller('AdapterRunDetailStatusController', function($scope, $state, myAdapterApi, CanvasFactory, MyAppDAGService, DashboardHelper, MyDataSource, MyMetricsQueryHelper) {

    var datasrc = new MyDataSource($scope);

    $scope.nodes = [];
    var params = {
      namespace: $state.params.namespace,
      adapter: $state.params.adapterId,
      scope: $scope
    };

    $scope.cloneAdapter = function() {
      if ($scope.config) {
        $state.go('adapters.create', {
          data: $scope.config,
          type: $scope.config.template
        });
      }
    };

    myAdapterApi.get(params)
      .$promise
      .then(function(res) {
        $scope.config = {
          name: $state.params.adapterId,
          template: res.template,
          description: res.description,
          config: {
            source: res.config.source,
            sink: res.config.sink,
            transforms: res.config.transforms,
            instances: res.instance,
            schedule: res.config.schedule
          }
        };

        MyAppDAGService.metadata.name = res.name;
        MyAppDAGService.metadata.description = res.description;
        MyAppDAGService.metadata.template.type = res.template;
        if (res.template === 'ETLBatch') {
          MyAppDAGService.metadata.template.schedule = res.config.schedule;
        } else if (res.template === 'ETLRealtime') {
          MyAppDAGService.metadata.template.instances = res.config.instances;
        }

        $scope.source = res.config.source;
        $scope.sink = res.config.sink;
        $scope.transforms = res.config.transforms;
        $scope.nodes = CanvasFactory.getNodes(res.config);
        $scope.nodes.forEach(function(node) {
          MyAppDAGService.addNodes(node, node.type);
        });

        MyAppDAGService.connections = CanvasFactory.getConnectionsBasedOnNodes($scope.nodes);


      });

    if ($scope.runs.length === 0) {
      return;
    }
    var context = 'namespace.' + $state.params.namespace +
                  '.adapter.' + $state.params.adapterId +
                  '.run.' + $scope.runs.selected.runid;
    var tagQueryParams = MyMetricsQueryHelper
                          .tagsToParams(
                            MyMetricsQueryHelper.contextToTags(context)
                          );
    var widget = {
      metric: {
        context: context,
        names: []
      },
      settings: {
        aggregate: 61
      },
      metricAlias: {

      }
    };

    datasrc.poll(
      {
        method: 'POST',
        _cdapPath: '/metrics/search?target=metric&' + tagQueryParams
      },
      function onMetricsDiscoverySuccess(res) {
        widget.metric.names = res;
        if (res.length > 0) {
          fetchMetricsData(widget);
        } else {
          $scope.formattedData = [];
        }
      }
    );
    function fetchMetricsData(widget) {
      DashboardHelper.fetchData(widget)
        .then(
          function onMetricsFetchSuccess() {
            if (!widget.formattedData || !widget.formattedData.columns) {
              return;
            }
            $scope.formattedData = {};
            $scope.isMetrics = false;
            $scope.formattedData = {};
            widget.formattedData.columns
                  .map(extractMetricsFromData.bind(null, $scope.formattedData));
            $scope.isMetrics = Object.keys($scope.formattedData).length > 0 ? true: false;
            $scope.formattedData = Object.keys($scope.formattedData)
              .map(function(key) {
                return $scope.formattedData[key];
              });
            $scope.formattedData = orderMetricsForDisplay($scope.formattedData);
          },
          function onMetricsFetchError() {
            console.log('Error in fetching values for metrics: ', widget.metric.names);
          }
        );
    }

    function orderMetricsForDisplay(data) {
      var source = {};
      var transforms = [];
      var sink = {};
      var returnArray = [];
      source = data.filter(function(metric) {
        return metric.type === 'source';
      });
      source = source[0];
      sink = data.filter(function(metric) {
        return metric.type === 'sink';
      });
      sink = sink[0];
      transforms = data.filter(function(metric) {
        return metric.type === 'transform';
      });
      returnArray = returnArray.concat(transforms);
      if (source) {returnArray.unshift(source);}
      if (sink) {returnArray.push(sink);}
      return returnArray;
    }


    function extractMetricsFromData(obj, metric) {
      var matches = ['records.in', 'records.out'];
      var match = matches.filter(function(m) {
        return metric[0].indexOf(m) !== -1;
      });
      if (match.length > 0) {
        var metricNameArray = metric[0].split('.');
        var metricKey = metric[0].substring(0, metric[0].lastIndexOf('.'));
        var inOut = metric[0].substring(
          metric[0].lastIndexOf('.') + 1,
          metric[0].length
        );
        obj[metricKey] =obj[metricKey] || {};
        if (metricNameArray.length === 5) {
          // No stage
          obj[metricKey].type = metricNameArray[1];
          obj[metricKey].stage = 0;
          obj[metricKey].node = metricNameArray[2];
        } else if (metricNameArray.length === 6) {
          obj[metricKey].type = metricNameArray[1];
          obj[metricKey].stage = metricNameArray[3];
          obj[metricKey].node = metricNameArray[2];
        }
        obj[metricKey].value = obj[metricKey].value || {};
        obj[metricKey].value[inOut] = metric[1];
      }
    }
  });
