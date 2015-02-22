angular.module(PKG.name + '.feature.streams')
  .controller('StreamsDetailStatusController', function($scope, $state, myHelpers, MyDataSource) {
    var dataSrc = new MyDataSource($scope);
    dataSrc.request({
      _cdapNsPath: '/streams/' + $state.params.streamId + '/info'
    })
      .then(function(stream) {
        $scope.stream = stream;
        $scope.schema = stream.format.schema.fields;
      });

    [
      {
        name: 'system.collect.bytes',
        scopeProperty: 'bytes'
      },
      {
        name: 'system.collect.events',
        scopeProperty: 'events'
      }
    ].forEach(fetchMetric);

    function fetchMetric(metric) {
      var path = '/metrics/query?metric=' +
                  metric.name +
                  '&context=ns.' +
                  $state.params.namespace +
                  '.stream.' +
                  $state.params.streamId;

      dataSrc.request({
        _cdapPath : path ,
        method: 'POST'
      })
        .then(function(metricData) {
          var data = myHelpers.objectQuery(metricData, 'series', 0, 'data', 0, 'value');
          $scope[metric.scopeProperty] = data;
        });
    }
  });
