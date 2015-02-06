angular.module(PKG.name + '.feature.admin').controller('AdminServiceDetailController',
function ($scope, $state, MyDataSource) {

    var myDataSrc = new MyDataSource($scope);

    myDataSrc.request({
      _cdapPathV2: '/system/services/' + $state.params.serviceName + '/instances'
    })
      .then(function(response) {
        $scope.instances = response;
      });

});
