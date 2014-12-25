angular.module(PKG.name + '.feature.admin')
  .controller('AdminController', function ($scope, $state, myNamespace) {
    $scope.toggleMenu = function(menu) {
      menu.showSubMenu = !menu.showSubMenu;
    };
    myNamespace.getList()
      .then(function(list) {
        $scope.nsList = list.map(generateNsObject);
      });
    function generateNsObject(item) {
      return {
        state: 'admin.namespace({nsId: "' + item.name +'" })',
        label: item.displayName,
        children: [
          {
            state: 'admin.namespace.settings({nsId: "' + item.name +'" })',
            label: 'Settings',
            children: []
          },
          {
            state: 'admin.namespace.users({nsId: "' + item.name +'" })',
            label: 'Users',
            children: []
          },
          {
            state: 'admin.namespace.datatypes({nsId: "' + item.name +'" })',
            label: 'Data Types',
            children: []
          },
          {
            state: 'admin.namespace.datasets({nsId: "' + item.name +'" })',
            label: 'Datasets',
            children: []
          },
          {
            state: 'admin.namespace.apps({nsId: "' + item.name +'" })',
            label: 'Apps',
            children: []
          }
        ]
      };
    }
  });
