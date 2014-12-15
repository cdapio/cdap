angular.module(PKG.name + '.feature.admin')
  .config(function($stateProvider, $urlRouterProvider) {
    $urlRouterProvider.when('/admin', '/admin/overview');
    $stateProvider
      .state('admin', {
        url: '/admin',
        templateUrl: '/assets/features/admin/templates/admin.html',
        controller: 'AdminController'
      })
        .state('admin.overview', {
          url: '/overview',
          templateUrl: '/assets/features/admin/templates/overview.html'
        })
        .state('admin.system', {
          url: '/system',
          templateUrl: '/assets/features/admin/templates/system.html'
        })
        .state('admin.security', {
          url: '/security',
          templateUrl: '/assets/features/admin/templates/security.html'
        })
        .state('admin.namespace', {
          url: '/ns/:namespaceId',
          resolve: {
            namespaceId: function() {
              return 'namespace1';
            }
          },
          templateUrl: '/assets/features/admin/templates/namespace.html'
        })
          .state('admin.namespace.settings', {
            url: '/settings',
            templateUrl: 'assets/features/admin/templates/namespace/settings.html'
          })
          .state('admin.namespace.users', {
            url: '/users',
            templateUrl: 'assets/features/admin/templates/namespace/users.html'
          })
          .state('admin.namespace.datatypes', {
            url: '/datatypes',
            templateUrl: 'assets/features/admin/templates/namespace/datatypes.html'
          })
          .state('admin.namespace.datasets', {
            url: '/datasets',
            templateUrl: 'assets/features/admin/templates/namespace/datasets.html'
          })
          .state('admin.namespace.apps', {
            url: '/apps',
            templateUrl: 'assets/features/admin/templates/namespace/apps.html'
          });
  });
