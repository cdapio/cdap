angular.module(PKG.name + '.feature.admin').controller('AdminNamespaceMetadataController',
function ($scope, $state, $alert, $timeout, MyDataSource) {

  var data = new MyDataSource($scope);
  var path = '/namespaces/' + $state.params.nsadmin;

  data.request({
    _cdapPath: path
  })
    .then(function(metadata) {
      $scope.metadata = metadata;
    });

  $scope.deleteApp = function(namespace) {
    data.request({
      _cdapPath: path,
      method: 'DELETE'
    }, function(res) {
      $alert({
        type: 'success',
        title: namespace,
        content: 'Namespace deleted successfully'
      });
      // FIXME: Have to avoid $timeout here. Un-necessary.
      $timeout(function() {
        $state.go('^');
      });
    });
  };

});
