angular.module(PKG.name + '.feature.mapreduce')
  .controller('MapreduceStatusController', function($scope, MyDataSource, $state, $rootScope, myHelpers) {
    var dataSrc = new MyDataSource($scope);

    var pathIn = '/metrics/query?metric=system.process.entries.in&context=namespace.' + $state.params.namespace + '.app.' + $state.params.appId + '.mapreduce.' + $state.params.programId + '.tasktype.',
        pathOut = '/metrics/query?metric=system.process.entries.out&context=namespace.' + $state.params.namespace + '.app.' + $state.params.appId + '.mapreduce.' + $state.params.programId + '.tasktype.';

    dataSrc.poll({
      _cdapPath: pathIn + 'm',
      method: 'POST'
    }, function(res) {
      $scope.mapperTaskIn = myHelpers.objectQuery(res, 'series', 0, 'data', 0, 'value' );
    });

    dataSrc.poll({
      _cdapPath: pathIn + 'r',
      method: 'POST'
    }, function(res) {
      $scope.reduceTaskIn = myHelpers.objectQuery(res, 'series', 0, 'data', 0, 'value' );
    });

    dataSrc.poll({
      _cdapPath: pathOut + 'm',
      method: 'POST'
    }, function(res) {
      $scope.mapperTaskOut = myHelpers.objectQuery(res, 'series', 0, 'data', 0, 'value' );
    });

    dataSrc.poll({
      _cdapPath: pathOut + 'r',
      method: 'POST'
    }, function(res) {
      $scope.reduceTaskOut = myHelpers.objectQuery(res, 'series', 0, 'data', 0, 'value' );
    });

  });
