angular.module(PKG.name + '.feature.admin').controller('AdminServiceDetailController',
function ($scope, $state, MyDataSource) {
    $scope.basePath = '/system/services/' + $state.params.serviceName;
    var myDataSrc = new MyDataSource($scope);

    myDataSrc.request({
      _cdapPath: $scope.basePath  + '/instances'
    })
      .then(function(response) {
        $scope.instances = response;
      });
});
