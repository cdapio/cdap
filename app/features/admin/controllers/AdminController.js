angular.module(PKG.name + '.feature.admin')
  .controller('AdminController', function ($scope, $state, myNamespace) {

    myNamespace.getList()
      .then(function(list) {
        $scope.nsList = list.map(generateNsObject);
      });

    // wether or not to show submenus
    $scope.submenu = {
      system: false,
      security: false
    };
    function generateNsObject(item) {
      return {
        state: '',
        label: item.name,
        children: [
          {
            state: 'admin.namespace.detail.metadata({nsadmin: "' + item.id +'" })',
            label: 'Metadata',
            children: []
          },
          {
            state: 'admin.namespace.detail.settings({nsadmin: "' + item.id +'" })',
            label: 'Settings',
            children: []
          },
          {
            state: 'admin.namespace.detail.users({nsadmin: "' + item.id +'" })',
            label: 'Users',
            children: []
          },
          {
            state: 'admin.namespace.detail.datatypes({nsadmin: "' + item.id +'" })',
            label: 'Data Types',
            children: []
          },
          {
            state: 'admin.namespace.detail.datasets({nsadmin: "' + item.id +'" })',
            label: 'Datasets',
            children: []
          },
          {
            state: 'admin.namespace.detail.apps({nsadmin: "' + item.id +'" })',
            label: 'Apps',
            children: []
          }
        ]
      };
    }
  });
