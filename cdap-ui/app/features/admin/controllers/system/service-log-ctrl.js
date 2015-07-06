angular.module(PKG.name + '.feature.admin').controller('SystemServiceLogController',
function ($scope, $state, MyDataSource) {

    var myDataSrc = new MyDataSource($scope);

    myDataSrc.request({
      _cdapPath: '/system/services/' + encodeURIComponent($state.params.serviceName) + '/logs/next?&maxSize=50'
    })
      .then(function(response) {
        $scope.logs = response;
      });

});
