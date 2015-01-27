angular.module(PKG.name + '.feature.admin')
  .config(function($stateProvider, $urlRouterProvider, MYAUTH_ROLE) {

    $stateProvider
      .state('admin', {
        abstract: true,
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'management'
        },
        url: '/admin',
        templateUrl: '/assets/features/admin/templates/admin.html',
        controller: 'AdminController'
      })

        .state('admin.overview', {
          url: '',
          templateUrl: '/assets/features/admin/templates/overview.html'
        })

        .state('admin.system', {
          url: '/system',
          templateUrl: '/assets/features/admin/templates/system.html'
        })

          .state('admin.system.instance', {
            url: '/instance',
            templateUrl: '/assets/features/admin/templates/instance.html'
          })

        .state('admin.security', {
          url: '/security',
          templateUrl: '/assets/features/admin/templates/security.html'
        })

        .state('admin.namespace', {
          abstract: true,
          url: '/namespace',
          template: '<ui-view/>'
        })
          .state('admin.namespace.create', {
            url: '/create',
            templateUrl: '/assets/features/admin/templates/namespace/create.html',
            controller: 'NamespaceCreateController'
          })

          .state('admin.namespace.detail', {
            url: '/detail/:nsadmin',
            templateUrl: '/assets/features/admin/templates/namespace.html'
          })

            .state('admin.namespace.detail.metadata', {
              url: '/metadata',
              templateUrl: '/assets/features/admin/templates/namespace/metadata.html'
            })

            .state('admin.namespace.detail.settings', {
              url: '/settings',
              templateUrl: '/assets/features/admin/templates/namespace/settings.html'
            })

            .state('admin.namespace.detail.users', {
              url: '/users',
              templateUrl: '/assets/features/admin/templates/namespace/users.html'
            })

            .state('admin.namespace.detail.datatypes', {
              url: '/datatypes',
              templateUrl: '/assets/features/admin/templates/namespace/datatypes.html'
            })

            .state('admin.namespace.detail.datasets', {
              url: '/datasets',
              templateUrl: '/assets/features/admin/templates/namespace/datasets.html'
            })

            .state('admin.namespace.detail.apps', {
              url: '/apps',
              templateUrl: '/assets/features/admin/templates/namespace/apps.html'
            });
  });
