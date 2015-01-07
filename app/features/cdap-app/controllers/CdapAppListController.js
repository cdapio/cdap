angular.module(PKG.name + '.feature.cdap-app')
  .controller('CdapAppListController', function CdapAppList( $timeout, $scope, MyDataSource, fileUploader, $alert, $state, $stateParams) {
    var data = new MyDataSource($scope);

    data.request({
      _cdapNsPath: '/apps/',
      method: 'GET',
    }, function(res) {
      $scope.apps = res;
    });

    $scope.onFileSelected = function(files) {
      for (var i = 0; i < files.length; i++) {
        fileUploader.upload({
          path: '/namespaces/' + $state.params.namespace + '/apps',
          file: files[i]
        })
          .then(success,error);
      }

      function success() {
        $alert({
          type: 'success',
          title: 'Upload success!',
          content: 'The application has been uploaded successfully!'
        });
        $state.reload();
      }

      // Independent xhr request. Failure case will not be handled by $rootScope.
      function error(err) {
        $alert({
          type: 'danger',
          title: 'Upload failed!',
          content: err || ''
        });
      }
    };


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
