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
            parent: 'admin',
            url: '/system/instance',
            templateUrl: '/assets/features/admin/templates/system/instance.html',
            controller: 'InstanceController'
          })
          .state('admin.system.services', {
            parent: 'admin',
            url: '/system/services',
            templateUrl: '/assets/features/admin/templates/system/services.html',
            controller: 'AdminServicesController'
          })
            .state('admin.system.services.detail', {
              url: '/detail',
              templateUrl: '/assets/features/admin/templates/system/service-detail.html'
            })

          .state('admin.system.notifications', {
            parent: 'admin',
            url: '/system/notifications',
            templateUrl: '/assets/features/admin/templates/system/notifications.html'
          })
            .state('admin.system.notifications.add', {
              onEnter: function ($state, $modal) {
                $modal({
                  template: '/assets/features/admin/templates/partials/create-notif.html'
                }).$promise.then(function () {
                  $state.go('^', $state.params);
                });
              }
            })
        .state('admin.security', {
          url: '/security',
          templateUrl: '/assets/features/admin/templates/security.html'
        })

          .state('admin.security.permissions', {
            parent: 'admin',
            url: '/security/permissions',
            templateUrl: '/assets/features/admin/templates/security/permissions.html'
          })
          .state('admin.security.tokens', {
            parent: 'admin',
            url: '/security/tokens',
            templateUrl: '/assets/features/admin/templates/security/tokens.html'
          })
            .state('admin.security.tokens.revoke', {
              onEnter: function ($state, $modal) {
                $modal({
                  template: '/assets/features/admin/templates/partials/revoke-tokens.html'
                }).$promise.then(function () {
                  $state.go('^', $state.params);
                });
              }
            })
          .state('admin.security.reports', {
            parent: 'admin',
            url: '/security/reports',
            templateUrl: '/assets/features/admin/templates/security/reports.html'
          })
          .state('admin.security.logs', {
            parent: 'admin',
            url: '/security/logs',
            templateUrl: '/assets/features/admin/templates/security/logs.html',
            controller: 'AuditLogsController'
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
              templateUrl: '/assets/features/admin/templates/namespace/datatypes.html',
              controller: 'DatatypesController'
            })

            .state('admin.namespace.detail.datasets', {
              url: '/datasets',
              templateUrl: '/assets/features/admin/templates/namespace/datasets.html',
              controller: 'DatasetsController'
            })

            .state('admin.namespace.detail.apps', {
              url: '/apps',
              templateUrl: '/assets/features/admin/templates/namespace/apps.html'
            });
  });
