angular.module(PKG.name + '.feature.admin')
  .controller('AdminController', function ($scope, $state, myNamespace) {

    myNamespace.getList()
      .then(function(list) {
        $scope.nsList = list;
        handleSubmenus();
      });

    // whether or not to show submenus
    $scope.submenu = {
      system: false,
      security: false,
      namespaces: false,
      perNs: {}
    };

    $scope.$on('$stateChangeSuccess', handleSubmenus);

    function handleSubmenus() {
      if (!$scope.submenu.security) {
        $scope.submenu.security = $state.is('admin.security') || $state.includes('admin.security.**');
      }
      if (!$scope.submenu.system) {
        $scope.submenu.system = $state.is('admin.system') || $state.includes('admin.system.**');
      }

      if (!$scope.submenu.namespaces) {
        $scope.submenu.namespaces = $state.is('admin.namespace') || $state.includes('admin.namespace.**');
      }

      if($state.params.nsadmin) {
        $scope.submenu.perNs[$state.params.nsadmin] = true;
      }
    }


  });
