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

angular.module(PKG.name+'.feature.dashboard')
.controller('WidgetTableCtrl', function ($scope, MyDataSource, MyChartHelpers, MyMetricsQueryHelper) {

  $scope.metrics = $scope.wdgt.metric;
  var dataSrc = new MyDataSource($scope);

  dataSrc.request(
    {
      _cdapPath: '/metrics/query',
      method: 'POST',
      body: MyMetricsQueryHelper.constructQuery(
        'qid',
        MyMetricsQueryHelper.contextToTags($scope.metrics.context),
        $scope.metrics
      )
    }
  )
    .then(function(res) {
      var processedData = MyChartHelpers.processData(
        res,
        'qid',
        $scope.metrics.names,
        $scope.metrics.resolution
      );
      processedData = MyChartHelpers.c3ifyData(processedData, $scope.metrics, {});
      var tableData = [];
      processedData.xCoords.forEach(function(timestamp, index) {
        if (index === 0) {
          // the first index of each column is just 'x' or the metric name
          return;
        }
        var rowData = [timestamp];
        processedData.columns.forEach(function(column) {
          // If it begins with 'x', it is timestamps
          if (column.length && column[0] !== 'x') {
            rowData.push(column[index]);
          }
        });
        tableData.push(rowData);
      });
      $scope.tableData = tableData;
    });
});
