angular.module(PKG.name + '.feature.admin')
  .controller('AdminNamespaceAppMetadataController', function ($scope, $state, $dropdown, $alert, $timeout, MyDataSource) {

    var data = new MyDataSource($scope);

    data.request({
      _cdapPath: '/namespaces/' + $state.params.nsadmin + '/apps/' + $state.params.appId
    })
      .then(function(apps) {
        $scope.apps = apps;

      });

  $scope.appDdClick = function (event) {

    var toggle = angular.element(event.target);
    if(!toggle.hasClass('dropdown-toggle')) {
      toggle = toggle.parent();
    }

    if(toggle.parent().hasClass('open')) {
      return;
    }

    var scope = $scope.$new(),
        dd = $dropdown(toggle, {
          template: 'assets/features/admin/templates/partials/app-dd.html',
          animation: 'am-flip-x',
          trigger: 'manual',
          prefixEvent: 'nsadmin-app-dd.hide',
          scope: scope
        });

    dd.$promise.then(function(){
      dd.show();
    });

    scope.$on('nsadmin-app-dd.hide', function () {
      dd.destroy();
    });

  };

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
