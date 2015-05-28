angular.module(PKG.name + '.feature.admin')
  .controller('AdminNamespaceSettingController', function ($scope, MyDataSource, $state, $alert, $timeout, myNamespace) {

    var dataSrc = new MyDataSource($scope);
    $scope.loading = false;

    dataSrc.request({
      _cdapPath: '/namespaces/' + $state.params.nsadmin
    })
    .then(function (res) {
      $scope.description = res.description;
    });

    $scope.save = function() {

      dataSrc.request({
        _cdapPath: '/namespaces/' + $state.params.nsadmin + '/properties',
        method: 'PUT',
        body: {
          'description': $scope.description
        }
      })
      .then(function () {
        $alert({
          type: 'success',
          content: 'Namespace have successfully updated'
        });
      });

    };

    $scope.deleteNamespace = function() {
      $scope.loading = true;

      dataSrc.request({
        _cdapPath: '/unrecoverable/namespaces/' + $state.params.nsadmin,
        method: 'DELETE'
      })
      .then(function () {
        myNamespace.getList(true);
        $timeout(function() {
          $state.go('admin.overview', {}, {reload: true});
          $alert({
            type: 'success',
            content: 'You have successfully deleted a namespace.'
          });
        }, 500);
      });
    };

  });
