angular.module(PKG.name + '.feature.applications')
  .controller('ApplicationStatusController', function($scope, $state, MyDataSource) {
    var data = new MyDataSource($scope);
    console.log($state.params);
    var appId = $state.params.appId;
    data.fetch({
      _cdap: "GET /apps/" + appId
    }, function(res) {
      $scope.app = res;
    });
  });
