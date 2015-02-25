angular.module(PKG.name + '.feature.flows')
  .controller('FlowsRunLogController', function() {

    var dataSrc = new MyDataSource($scope),
        basePath = '/apps/' + $state.params.appId + '/flows/' + $state.params.programId;

    $scope.logs = [];

    dataSrc.poll({
      _cdapNsPath: basePath + '/logs/next?fromOffset=-1&maxSize=50'
    }, function(res) {
      $scope.logs = res;
    });

});
