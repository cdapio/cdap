angular.module(PKG.name + '.feature.apps')
  .controller('CdapAppListController', function CdapAppList( $timeout, $scope, MyDataSource, myAppUploader, $alert, $state) {
    var data = new MyDataSource($scope);

    $scope.apps = data.request({
      _cdapNsPath: '/apps/'
    });
    $scope.onFileSelected = myAppUploader.upload;


    $scope.deleteApp = function(app) {
      data.request({
        _cdapNsPath: '/apps/' + app,
        method: 'DELETE'
      }, function(res) {
        $alert({
          type: 'success',
          title: app,
          content: 'App deleted successfully'
        });
        // FIXME: Have to avoid $timeout here. Un-necessary.
        $timeout($state.reload);
      })
    }
  });
