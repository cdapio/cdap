angular.module(PKG.name + '.feature.datasets')
  .controller('CdapDatasetDetailStatusController', function($scope, MyDataSource, $state, myHelpersUtil) {
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

    var query = myHelpersUtil.objectQuery;

    dataSrc.request({
      _cdapPathV2: '/data/datasets'
    })
      .then(function(res) {
        var match = res.filter(function(ds) {
          return ds.name.indexOf(currentDataset) > -1;
        });
        $scope.schema = (query(
            JSON.parse( query(match, 0, "properties", "schema") ),
            "fields"
          ) || [])
          .map(function(field) {
            if (angular.isArray(field.type)) {
              field.type = flattenFieldType(field.type);
            }
            return field;
          });
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
