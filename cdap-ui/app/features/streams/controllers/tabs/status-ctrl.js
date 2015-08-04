angular.module(PKG.name + '.feature.streams')
  .controller('StreamDetailStatusController', function($scope, $state, myHelpers, MyDataSource, myStreamApi) {
    var dataSrc = new MyDataSource($scope);
    this.storage = null;
    this.events = null;
    var params = {
      namespace: $state.params.namespace,
      streamId: $state.params.streamId,
      scope: $scope
    };
    myStreamApi.get(params)
      .$promise
      .then(function (res) {
        this.schema = res.format.schema.fields;
      }.bind(this));

    [
      {
        name: 'system.collect.bytes',
        scopeProperty: 'storage'
      },
      {
        name: 'system.collect.events',
        scopeProperty: 'events'
      }
    ].forEach(fetchMetric.bind(this));

    function fetchMetric(metric) {
      var path = '/metrics/query?metric=' + metric.name +
                  '&tag=ns:' + $state.params.namespace +
                  '&tag=stream:' + $state.params.streamId;

      dataSrc.poll({
        _cdapPath : path ,
        method: 'POST'
      }, function(metricData) {
          var data = myHelpers.objectQuery(metricData, 'series', 0, 'data', 0, 'value');
          this[metric.scopeProperty] = data;

          /**
          * FIX ME: JavaScript largest possible number: 9007199254740992
          * The backend stores number as 64 bit long. Max: 9,223,372,036,854,775,807
          * JavaScript is missing 3 digits. Beyond the JS number, it will round-of the value
          **/

      }.bind(this));
    }
  });
