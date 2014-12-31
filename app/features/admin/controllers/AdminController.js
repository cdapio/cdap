angular.module(PKG.name + '.feature.admin')
  .controller('AdminController', function ($scope, $state, myNamespace) {

    myNamespace.getList()
      .then(function(list) {
        $scope.nsList = list.map(generateNsObject);
      });

    function generateNsObject(item) {
      return {
        state: '',
        label: item.displayName,
        children: [
          {
            state: 'admin.namespace.detail.settings({nsadmin: "' + item.displayName +'" })',
            label: 'Settings',
            children: []
          },
          {
            state: 'admin.namespace.detail.users({nsadmin: "' + item.displayName +'" })',
            label: 'Users',
            children: []
          },
          {
            state: 'admin.namespace.detail.datatypes({nsadmin: "' + item.displayName +'" })',
            label: 'Data Types',
            children: []
          },
          {
            state: 'admin.namespace.detail.datasets({nsadmin: "' + item.displayName +'" })',
            label: 'Datasets',
            children: []
          },
          {
            state: 'admin.namespace.detail.apps({nsadmin: "' + item.displayName +'" })',
            label: 'Apps',
            children: []
          }
        ]
      };
    }
  });
