angular.module(PKG.name + '.feature.admin').controller('AdminServicesController',
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