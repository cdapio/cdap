angular.module(PKG.name + '.feature.flows')
  .controller('FlowsDetailController', function($scope, MyDataSource, $state, FlowDiagramData, myHelpers, $timeout) {
    $scope.status = null;
    var dataSrc = new MyDataSource($scope),
        basePath = '/apps/' + $state.params.appId + '/flows/' + $state.params.programId;


    dataSrc.poll({
      _cdapNsPath: basePath + '/runs?status=running'
    }, function(res) {
        $scope.activeRuns = res.length;
      });


    $scope.do = function(action) {
      $scope.status = action;
      dataSrc.request({
        _cdapNsPath: basePath + '/' + action,
        method: 'POST'
      }).then(function() {
        $timeout(function() {
          $scope.status = null;
          $state.go($state.current, $state.params, { reload: true });
        });
      });
    };
  });
