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
