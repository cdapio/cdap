/**
 * HomeCtrl
 */

angular.module(PKG.name+'.feature.home').controller('HomeCtrl',
function ($scope, $alert, MyDataSource) {

  var dataSrc = new MyDataSource($scope);
  $scope.apps = [];
  dataSrc.fetch({
    _cdap: 'GET /apps/'
  }, function(res) {
    $scope.apps = res;
    console.log('Apps: ', $scope.apps);
  });

});
