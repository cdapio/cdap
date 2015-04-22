angular.module(PKG.name + '.feature.datasets')
  .controller('CdapDatasetDetailStatusController',
    function($scope, MyDataSource, $state, myHelpers) {
      $scope.writes = 0;
      $scope.reads = 0;
      $scope.transactions = 0;
      var query = myHelpers.objectQuery;
      var dataSrc = new MyDataSource($scope),
          currentDataset = $state.params.datasetId;

      [
        {
          name: 'system.dataset.store.bytes',
          scopeProperty: 'storage'
        },
        {
          name: 'system.dataset.store.reads',
          scopeProperty: 'reads'
        },
        {
          name: 'system.dataset.store.writes',
          scopeProperty: 'writes'
        }
      ].forEach(pollMetric);

      function pollMetric(metric) {
        // A temporary way to get the rate of a metric for a dataset.
        // Ideally this would be batched for datasets/streams
        var path = '/metrics/query?metric=' +
                    metric.name +
                    '&context=namespace.' +
                    $state.params.namespace +
                    '.dataset.' +
                    currentDataset +
                    '&start=now-1s&end=now-1s&resolution=1s';

        dataSrc.poll({
          _cdapPath : path ,
          method: 'POST'
        }, function(metricData) {
          var data = query(metricData, 'series', 0, 'data', 0, 'value');
          $scope[metric.scopeProperty] = data;
        });
      }

      dataSrc.request({
        _cdapNsPath: '/data/explore/tables/dataset_' + currentDataset + '/info'
      })
        .then(function(res) {

          $scope.schema = query(res, 'schema');

        });

  });
