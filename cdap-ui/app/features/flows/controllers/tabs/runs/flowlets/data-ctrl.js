angular.module(PKG.name + '.feature.flows')
  .controller('FlowletDetailDataController', function($state, $scope, MyDataSource, myHelpers) {
    var dataSrc = new MyDataSource($scope);
    var flowletid = $state.params.flowletid;
    $scope.datasets = [];

    // Initialize
    dataSrc
      .request({
        _cdapNsPath: '/apps/' + $state.params.appId+  '/flows/' + $state.params.programId
      })
      .then(function (res) {
        var obj = [];
        var datasets = myHelpers.objectQuery(res, 'flowlets', flowletid, 'flowletSpec', 'dataSets');

        angular.forEach(datasets, function (v) {
          obj.push({
            name: v
          });
        });

        $scope.datasets = obj;

        angular.forEach($scope.datasets, function (dataset) {
          dataSrc
            .poll({
              _cdapPath: '/metrics/query?context=namespace.' + $state.params.namespace
                        + '.dataset.' + dataset.name
                        + '.app.' + $state.params.appId
                        + '.flow.' + $state.params.programId
                        + '&metric=system.dataset.store.reads',
              method: 'POST'
            }, function(res) {
              dataset.reads = res.series[0].data[0].value;
            });

          dataSrc
            .poll({
              _cdapPath: '/metrics/query?context=namespace.' + $state.params.namespace
                        + '.dataset.' + dataset.name
                        + '.app.' + $state.params.appId
                        + '.flow.' + $state.params.programId
                        + '&metric=system.dataset.store.writes',
              method: 'POST'
            }, function(res) {
              dataset.writes = res.series[0].data[0].value;
            });
        });

      });

  });
