angular.module(PKG.name + '.feature.admin')
  .controller('AdminController', function ($scope, $state, myNamespace) {

    myNamespace.getList()
      .then(function(list) {
        $scope.nsList = list.map(generateNsObject);
        handleSubmenus();
      });

    // whether or not to show submenus
    $scope.submenu = {
      system: false,
      security: false,
      namespaces: false
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
    }


    function generateNsObject(item) {
      return {
        state: '',
        label: item.name,
        children: [
          {
            state: 'admin.namespace.detail.metadata({nsadmin: "' + item.id +'" })',
            matchingState: 'admin.namespace.detail.metadata.**',
            label: 'Metadata',
            children: []
          },
          // {
          //   state: 'admin.namespace.detail.settings({nsadmin: "' + item.id +'" })',
          //   matchingState: 'admin.namespace.detail.settings.**',
          //   label: 'Settings',
          //   children: []
          // },
          // {
          //   state: 'admin.namespace.detail.users({nsadmin: "' + item.id +'" })',
          //   matchingState: 'admin.namespace.detail.users.**',
          //   label: 'Users',
          //   children: []
          // },
          // {
          //   state: 'admin.namespace.detail.datatypes({nsadmin: "' + item.id +'" })',
          //   matchingState: 'admin.namespace.detail.datatypes.**',
          //   label: 'Data Types',
          //   children: []
          // },
          // {
          //   state: 'admin.namespace.detail.datasets({nsadmin: "' + item.id +'" })',
          //   matchingState: 'admin.namespace.detail.datasets.**',
          //   label: 'Datasets',
          //   children: []
          // },
          {
            state: 'admin.namespace.detail.apps({nsadmin: "' + item.id +'" })',
            matchingState: 'admin.namespace.detail.apps.**',
            label: 'Apps',
            children: []
          }
        ]
      };
    }
  });
