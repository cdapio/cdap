angular.module(PKG.name + '.feature.cdap-app')
  .controller('CdapAppListController', function CdapAppList($scope, MyDataSource, fileUploader, $alert, $state, $stateParams) {
    var data = new MyDataSource($scope);

    data.request({
      _cdapNsPath: '/apps/',
      method: 'GET',
    }, function(res) {
      $scope.apps = res;
    });

    $scope.onFileSelected = function(files, event) {
      for (var i = 0; i < files.length; i++) {
        fileUploader.upload({
          path: '/namespaces/' + $state.params.namespace + '/apps',
          file: files[i]
        })
          .then(
            function success() {
              $alert({
                type: 'success',
                title: 'Upload sucess!',
                message: 'The file is uploaded successfully!'
              });

              $state.transitionTo($state.current, $stateParams, {
                  reload: true,
                  inherit: false,
                  notify: true
              });
            },
            function error() {
              $alert({
                type: 'error',
                title: 'Boink!',
                message: 'Something is goofed up!'
              });
            }
          );
      }
    }
  });
