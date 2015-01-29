angular.module(PKG.name + '.feature.apps')
  .controller('CdapAppListController', function CdapAppList( $timeout, $scope, MyDataSource, myAppUploader, $alert, $state) {
    var data = new MyDataSource($scope);

    data.request({
      _cdapNsPath: '/apps/'
    })
      .then(function(apps) {
        $scope.apps = apps;
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
      });
    };
  });
