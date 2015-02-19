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
          name: 'system.dataset.store.ops',
          scopeProperty: 'bytes'
        },
        {
          name: 'system.dataset.store.reads',
          scopeProperty: 'reads'
        },
        {
          name: 'system.dataset.store.writes',
          scopeProperty: 'writes'
        }
      ].forEach(fetchMetric);

      function fetchMetric(metric) {
        var path = '/metrics/query?metric=' +
                    metric.name +
                    '&context=ns.' +
                    $state.params.namespace +
                    '.ds.' +
                    $state.params.datasetId;

        dataSrc.request({
          _cdapPath : path ,
          method: 'POST'
        })
          .then(function(metricData) {
            console.log("Metrics: ", metricData);
            var data = myHelpers.objectQuery(metricData, 'series', 0, 'data', 0, 'value');
            $scope[metric.scopeProperty] = data;
            console.log(metric.scopeProperty, data);
          });
      }

      var query = myHelpers.objectQuery;
      // Temporary API until we get a status API for each dataset.
      dataSrc.request({
        _cdapPathV2: '/data/datasets'
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
