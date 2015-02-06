angular.module(PKG.name + '.feature.admin').controller('AdminNamespaceAppMetadataController',
function ($scope, $state, $alert, $timeout, MyDataSource) {

  var data = new MyDataSource($scope);
  var path = '/namespaces/' + $state.params.nsadmin + '/apps/' + $state.params.appId;

  data.request({
    _cdapPath: path
  })
    .then(function(apps) {
      $scope.apps = apps;
    });

  $scope.deleteApp = function(app) {
    data.request({
      _cdapPath: '/namespaces/' + $state.params.nsadmin + '/apps/' + $state.params.appId,
      method: 'DELETE'
    }, function(res) {
      $alert({
        type: 'success',
        title: app,
        content: 'App deleted successfully'
      });
      // FIXME: Have to avoid $timeout here. Un-necessary.
      $timeout(function() {
        $state.go('^.apps');
      });
    });
  };

});
