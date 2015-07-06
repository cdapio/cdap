angular.module(PKG.name + '.feature.admin').controller('SystemServicesController',
function ($scope, MyDataSource) {

  $scope.services = [];

  var myDataSrc = new MyDataSource($scope);

  myDataSrc.request({
    _cdapPath: '/system/services'
  })
    .then(function(response) {
      $scope.services = response;
    });

});