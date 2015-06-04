angular.module(PKG.name + '.feature.streams')
  .controller('StreamsDetailStatusController', function($scope, $state, myHelpers, MyDataSource, myStreamApi) {
    var dataSrc = new MyDataSource($scope);

    var params = {
      namespace: $state.params.namespace,
      streamId: $state.params.streamId,
      scope: $scope
    };
    myStreamApi.get(params)
      .$promise
      .then(function (res) {
        $scope.schema = res.format.schema.fields;
      });

    [
      {
        name: 'system.collect.bytes',
        scopeProperty: 'storage'
      },
      {
        name: 'system.collect.events',
        scopeProperty: 'events'
      }
    ].forEach(fetchMetric);

    function fetchMetric(metric) {
      var path = '/metrics/query?metric=' + metric.name +
                  '&tag=ns:' + $state.params.namespace +
                  '&tag=stream:' + $state.params.streamId;

      dataSrc.poll({
        _cdapPath : path ,
        method: 'POST'
      }, function(metricData) {
          var data = myHelpers.objectQuery(metricData, 'series', 0, 'data', 0, 'value');
          $scope[metric.scopeProperty] = data;
        });
    }
  });
