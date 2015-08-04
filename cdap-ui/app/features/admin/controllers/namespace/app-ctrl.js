var alertpromise;
angular.module(PKG.name + '.feature.admin').controller('NamespaceAppController',
function ($scope, $state, myAppUploader, MyDataSource, myNamespace, myAdapterApi, $alert, $timeout) {

  $scope.apps = [];
  $scope.nsname = myNamespace.getDisplayName($state.params.nsadmin);
  var myDataSrc = new MyDataSource($scope);
  var path = '/namespaces/' + $state.params.nsadmin + '/apps';
  myDataSrc.request({
    _cdapPath: path
  })
    .then(function(response) {
      $scope.apps = $scope.apps.concat(response);
    });
  var params = {
    namespace: $state.params.nsadmin,
    scope: $scope
  };
  myAdapterApi.list(params)
    .$promise
    .then(function(res) {
      if (!res.length) {
        return;
      }
      res.forEach(function(adapter) {
        adapter.type = 'Adapter';
        adapter.id = adapter.name;
      });
      $scope.apps = $scope.apps.concat(res);
    });

  $scope.deleteApp = function(id, type) {
    var p;
    if (type === 'Adapter') {
      p = angular.extend(params, {
        adapter: id
      });
      deleteAdapter(id, p);
    } else {
      deleteApp(id);
    }
  }

  function deleteAdapter(id, p) {
    myAdapterApi
      .delete(p)
      .$promise
      .then(
        function success() {
          var alertObj = {
            type: 'success',
            content: 'Adapter ' + id + ' delete successfully'
          }, e;
          if (!alertpromise) {
            alertpromise = $alert(alertObj);
            e = $scope.$on('alert.hide', function() {
              alertpromise = null;
              e();
            });
          }
          $state.reload();
        },
        function error() {
          var alertObj = {
            type: 'danger',
            content: 'Adapter ' + id + ' could not be deleted. Please check for errors.'
          }, e;
          if (!alertpromise) {
            alertpromise = $alert(alertObj);
            e = $scope.$on('alert.hide', function() {
              alertpromise = null;
              e();
            });
          }
        }
      );
  }

  function deleteApp(id) {
    myDataSrc.request({
      _cdapPath: path + '/' + id,
      method: 'DELETE'
    }, function() {
      $alert({
        type: 'success',
        title: id,
        content: 'App deleted successfully'
      });
      $state.reload();
    });
  }

  $scope.onFileSelected = function(files) {
    myAppUploader.upload(files, $state.params.nsadmin);
  };
});
