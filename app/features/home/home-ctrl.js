/**
 * HomeCtrl
 */

angular.module(PKG.name+'.feature.home').controller('HomeCtrl',
function ($scope, $alert, MyDataSource) {

  $scope.apps = [];

  var dataSrc = new MyDataSource($scope);

  dataSrc.fetch({
    _cdap: 'GET /apps/'
  }, function(res) {

    $scope.apps = res;
    console.log('Apps: ', $scope.apps);
  });

});
