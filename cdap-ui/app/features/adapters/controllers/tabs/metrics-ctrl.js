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
  .controller('AdaptersDetailMetricsController', function($scope, AdapterDetail, MyDataSource, $filter, $state, MyMetricsQueryHelper) {

    $scope.metrics = [];

    var dataSrc = new MyDataSource($scope);
    var filter = $filter('filter');

    var metricParams = {
      namespace: $state.params.namespace,
      app: $state.params.adapterId
    };

    metricParams = MyMetricsQueryHelper.tagsToParams(metricParams);

    var metricBasePath = '/metrics/search?target=metric&' + metricParams;

    var stagesArray = [AdapterDetail.source].concat(AdapterDetail.transforms, AdapterDetail.sinks);
    stagesArray = stagesArray.map(function (n, i) { return n + '.' + (i+1); });


    dataSrc.poll({
      method: 'POST',
      _cdapPath: metricBasePath
    }, function (res) {
      if (res.length > 0) {
        var metricQuery = [];

        angular.forEach(stagesArray, function (node) {
          metricQuery = metricQuery.concat(filter(res, node));
        });

        if (metricQuery.length === 0) { return; }

        dataSrc.request({
          method: 'POST',
          _cdapPath: '/metrics/query?' + metricParams + '&metric=' + metricQuery.join('&metric=')
        }).then(function (metrics) {
          var metricObj = {};

          angular.forEach(metrics.series, function (metric) {
            var split = metric.metricName.split('.');
            var key = split[2] + '.' + split[3];

            if (!metricObj[key]) {
              metricObj[key] = {
                nodeType: split[1],
                nodeName: split[2],
                stage: +split[3]
              };
            }

            if (split[5] === 'in') {
              metricObj[key].recordsIn = metric.data[0].value;
            } else if (split[5] === 'out') {
              metricObj[key].recordsOut = metric.data[0].value;
            }

          });

          var metricsArr = [];
          angular.forEach(metricObj, function (val) {
            metricsArr.push(val);
          });

          $scope.metrics = metricsArr;
        });
      }

    });


  });
