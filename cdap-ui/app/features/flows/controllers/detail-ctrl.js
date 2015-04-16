angular.module(PKG.name + '.feature.flows')
  .controller('FlowsDetailController', function($scope, MyDataSource, $state, FlowDiagramData, myHelpers, $timeout) {
    var dataSrc = new MyDataSource($scope),
        basePath = '/apps/' + $state.params.appId + '/flows/' + $state.params.programId;



    dataSrc.poll({
      _cdapNsPath: basePath + '/runs?status=running'
    }, function(res) {
        $scope.activeRuns = res.length;
      });


    $scope.do = function(action) {
      dataSrc.request({
        _cdapNsPath: basePath + '/' + action,
        method: 'POST'
      }).then(function() {
        $timeout(function() {
          $state.go($state.current, $state.params, { reload: true });
        });
      });
    };
  });
