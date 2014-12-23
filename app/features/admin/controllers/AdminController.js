angular.module(PKG.name + '.feature.admin')
  .controller('AdminController', function ($scope, $state, myNamespace) {
    $scope.toggleMenu = function(menu) {
      menu.showSubMenu = !menu.showSubMenu;
    };
    myNamespace.getList()
      .then(function(list) {
        $scope.nsList = list.map(function(item) {
          return generateNsObject(item);
        });
      });
    function generateNsObject(item) {
      return {
        state: 'admin.namespace({namespaceId: "' + item.name +'" })',
        label: item.displayName,
        children: [
          {
            state: 'admin.namespace.settings({namespaceId: "' + item.name +'" })',
            label: 'Settings',
            children: []
          },
          {
            state: 'admin.namespace.users({namespaceId: "' + item.name +'" })',
            label: 'Users',
            children: []
          },
          {
            state: 'admin.namespace.datatypes({namespaceId: "' + item.name +'" })',
            label: 'Data Types',
            children: []
          },
          {
            state: 'admin.namespace.datasets({namespaceId: "' + item.name +'" })',
            label: 'Datasets',
            children: []
          },
          {
            state: 'admin.namespace.apps({namespaceId: "' + item.name +'" })',
            label: 'Apps',
            children: []
          }
        ]
      };
    }
  });
