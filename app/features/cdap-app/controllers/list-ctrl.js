angular.module(PKG.name + '.feature.cdap-app')
  .controller('CdapAppListController', function CdapAppList($scope, MyDataSource, fileUploader, $alert, $state, $stateParams) {
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
          message: 'The Application has been uploaded successfully!'
        });

        $state.transitionTo($state.current, $stateParams, {
            reload: true,
            inherit: false,
            notify: true
        });
      }

      function error(err) {
        $alert({
          type: 'error',
          title: 'Application upload failed!',
          message: err
        });
      }
    };
  });
