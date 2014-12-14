angular.module(PKG.name + '.feature.admin')
  .controller('AdminController', function ($scope, $state) {
    $scope.toggleMenu = function(menu) {
      menu.showSubMenu = !menu.showSubMenu;
    };
    $scope.nsList = [
      {
        state: 'admin.namespace({namespaceId: "namespace1"})',
        label: 'Namespace1',
        children: [
          {
            state: 'admin.namespace.settings()',
            label: 'Settings',
            children: []
          },
          {
            state: 'admin.namespace.users()',
            label: 'Users',
            children: []
          },
          {
            state: 'admin.namespace.datatypes()',
            label: 'Data Types',
            children: []
          },
          {
            state: 'admin.namespace.datasets()',
            label: 'Datasets',
            children: []
          },
          {
            state: 'admin.namespace.apps()',
            label: 'Apps',
            children: []
          }
        ]
      }
    ];

  });
