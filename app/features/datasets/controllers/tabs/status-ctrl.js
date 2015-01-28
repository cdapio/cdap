angular.module(PKG.name + '.feature.datasets')
  .controller('CdapDatasetDetailStatusController', function($scope, MyDataSource, $state) {
    $scope.writes = 0;
    $scope.reads = 0;
    $scope.transactions = 0;
    var dataSrc = new MyDataSource($scope),
        currentDataset = $state.params.datasetId;

    ['reads', 'writes', 'bytes'].forEach(fetchMetric);

    function fetchMetric(metric) {
      dataSrc.poll({
        _cdapPathV2: getMetricUrl(metric)
      }, function(res) {
        $scope[metric] = res.data;
      });
    }

    function getMetricUrl(metric) {
      return '/metrics/system/datasets/' + currentDataset + '/dataset.store.'+ metric +'?aggregate=true'
    }
  });
