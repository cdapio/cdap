angular.module(PKG.name + '.feature.datasets')
  .controller('CdapDatasetDetailStatusController',
    function($scope, MyDataSource, $state, myHelpers, $log) {
      $scope.writes = 0;
      $scope.reads = 0;
      $scope.transactions = 0;
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
                    $state.params.datasetId +
                    '&start=now-1s&end=now-1s&resolution=1s';

        dataSrc.poll({
          _cdapPath : path ,
          method: 'POST'
        }, function(metricData) {
          var data = myHelpers.objectQuery(metricData, 'series', 0, 'data', 0, 'value');
          $scope[metric.scopeProperty] = data;
        });
      }

      var query = myHelpers.objectQuery;
      // Temporary API until we get a status API for each dataset.
      dataSrc.request({
        _cdapNsPath: '/data/datasets'
      })
        .then(function(res) {
          var match = res.filter(function(ds) {
            return ds.name.indexOf(currentDataset) > -1;
          });
          try {
            $scope.schema = (
              query(JSON.parse( query(match, 0, 'properties', 'schema') || null ),'fields') || []
            )
              .map(function(field) {
                if (angular.isArray(field.type)) {
                  field.type = flattenFieldType(field.type);
                }
                return field;
              });
          } catch(e) {
            $log.error('Parsing schema for datasets failed! - ' + e);
          }
        });

      function flattenFieldType(type) {
        return type.reduce(function(prev, curr) {
          if (angular.isArray(prev)) {
            // Field of type null | (some.table.field.name | null)
            return curr + ' | ' + flattenFieldType(prev);
          } else if (angular.isObject(prev)) {
            if (prev.items) {
              // Field of type null | ([some.table.field.name | some.table.otherfield.name ] | null)
              return curr + ' | ('+ flattenFieldType(prev.items) + ')';
            } else {
              // Field of type null | some.table.field.name [some.table.field.type]. The some.table.field.name could be a foreign key?
              return curr + ' | ' + prev.name + ' [' + prev.type + '] ';
            }
          } else {
            // Field of type null | string
            return curr + ' | ' + prev;
          }
        });
      }
  });
