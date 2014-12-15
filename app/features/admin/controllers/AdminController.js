angular.module(PKG.name + '.feature.admin')
  .controller('AdminController', function ($scope, $state) {
    $scope.toggleMenu = function(menu) {
      menu.showSubMenu = !menu.showSubMenu;
    };
    $scope.nsList = [
      {
        //state: 'admin.namespace({namespaceId: "namespace1"})',
        label: 'Namespace1',
        children: [
          {
            state: 'admin.namespace.settings({namespaceId: "namespace1"})',
            label: 'Settings',
            children: []
          },
          {
            state: 'admin.namespace.users({namespaceId: "namespace1"})',
            label: 'Users',
            children: []
          },
          {
            state: 'admin.namespace.datatypes({namespaceId: "namespace1"})',
            label: 'Data Types',
            children: []
          },
          {
            state: 'admin.namespace.datasets({namespaceId: "namespace1"})',
            label: 'Datasets',
            children: []
          },
          {
            state: 'admin.namespace.apps({namespaceId: "namespace1"})',
            label: 'Apps',
            children: []
          }
        ]
      }
    ];

  });
