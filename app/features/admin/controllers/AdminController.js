angular.module(PKG.name + '.feature.admin')
  .controller('AdminController', function ($scope, $state, myNamespace) {
    $scope.toggleMenu = function(menu) {
      menu.showSubMenu = !menu.showSubMenu;
    };
    myNamespace.getList()
      .then(function(list) {
        debugger;
        $scope.nsList = list.map(generateNsObject);
      });
    function generateNsObject(item) {
      return {
        state: 'admin.namespace({nsId: "' + item.displayName +'" })',
        label: item.displayName,
        children: [
          {
            state: 'admin.namespace.settings({nsId: "' + item.displayName +'" })',
            label: 'Settings',
            children: []
          },
          {
            state: 'admin.namespace.users({nsId: "' + item.displayName +'" })',
            label: 'Users',
            children: []
          },
          {
            state: 'admin.namespace.datatypes({nsId: "' + item.displayName +'" })',
            label: 'Data Types',
            children: []
          },
          {
            state: 'admin.namespace.datasets({nsId: "' + item.displayName +'" })',
            label: 'Datasets',
            children: []
          },
          {
            state: 'admin.namespace.apps({nsId: "' + item.displayName +'" })',
            label: 'Apps',
            children: []
          }
        ]
      };
    }
  });
