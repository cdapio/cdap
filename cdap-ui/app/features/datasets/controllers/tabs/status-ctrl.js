angular.module(PKG.name + '.feature.datasets')
  .controller('CdapDatasetDetailStatusController',
    function($scope, MyDataSource, $state, myHelpers, MyMetricsQueryHelper, myExploreApi) {
      $scope.writes = 0;
      $scope.reads = 0;
      $scope.storage = 0;
      $scope.transactions = 0;
      var query = myHelpers.objectQuery;
      var dataSrc = new MyDataSource($scope),
          currentDataset = $state.params.datasetId,
          datasetTags = {
            namespace: $state.params.namespace,
            dataset: currentDataset
          };

      [
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
        var path = '/metrics/query?metric=' + metric.name +
                    '&' + MyMetricsQueryHelper.tagsToParams(datasetTags) +
                    '&start=now-1s&end=now-1s&resolution=1s';

        dataSrc.poll({
          _cdapPath : path ,
          method: 'POST'
        }, function(metricData) {
          var data = query(metricData, 'series', 0, 'data', 0, 'value');
          $scope[metric.scopeProperty] = data;
        });
      }

      dataSrc.poll({
        _cdapPath : '/metrics/query?metric=system.dataset.store.bytes' +
                    '&' + MyMetricsQueryHelper.tagsToParams(datasetTags) +
                    '&aggregate=true',
        method: 'POST'
      }, function(metricData) {
        var data = query(metricData, 'series', 0, 'data', 0, 'value');
        $scope.storage = data;
      });

      var params = {
        namespace: $state.params.namespace,
        table: 'dataset_' + currentDataset,
        scope: $scope
      };

      myExploreApi.getInfo(params)
        .$promise
        .then(function (res) {
          $scope.schema = query(res, 'schema');
        });

  });
