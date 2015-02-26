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
          templateUrl: '/assets/features/admin/templates/overview.html',
          controller: 'AdminOverviewController'
        })

        .state('admin.system', {
          abstract: true,
          url: '/system',
          template: '<ui-view/>'
        })
          .state('admin.system.overview', {
            url: '',
            templateUrl: '/assets/features/admin/templates/system.html'
          })
          .state('admin.system.instance', {
            url: '/instance',
            templateUrl: '/assets/features/admin/templates/system/instance.html',
            controller: 'AdminInstanceController'
          })

          .state('admin.system.services', {
            url: '/services',
            templateUrl: '/assets/features/admin/templates/system/services.html',
            controller: 'AdminServicesController'
          })
            .state('admin.system.services.detail', {
              parent: 'admin.system',
              url: '/services/detail/:serviceName',
              templateUrl: '/assets/features/admin/templates/system/service-detail.html',
              controller: 'AdminServiceDetailController'
            })
              .state('admin.system.services.detail.metadata', {
                url: '/metadata',
                templateUrl: '/assets/features/admin/templates/partials/service-detail-metadata.html'
              })
              .state('admin.system.services.detail.logs', {
                url: '/logs?filter',
                reloadOnSearch: false,
                template: '<my-log-viewer data-model="logs"></my-log-viewer>',
                controller: 'AdminServiceLogController'
              })

          .state('admin.system.notifications', {
            url: '/notifications',
            templateUrl: '/assets/features/admin/templates/system/notifications.html'
          })

        .state('admin.security', {
          abstract: true,
          url: '/security',
          template: '<ui-view/>'
        })
          .state('admin.security.overview', {
            url: '',
            templateUrl: '/assets/features/admin/templates/security.html'
          })
          .state('admin.security.permissions', {
            url: '/permissions',
            templateUrl: '/assets/features/admin/templates/security/permissions.html'
          })
          .state('admin.security.tokens', {
            url: '/tokens',
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
          .state('admin.security.logs', {
            url: '/logs',
            templateUrl: '/assets/features/admin/templates/security/logs.html',
            controller: 'AdminAuditLogsController'
          })

        .state('admin.namespace', {
          abstract: true,
          url: '/namespace',
          resolve: {
            nsList: function(myNamespace) {
              // This is to make sure that namespace list is available to
              // admin.namespace (sidebar)
              return myNamespace.getList(true);
            }
          },
          template: '<ui-view/>'
        })
          .state('admin.namespace.create', {
            url: '/create',
            templateUrl: '/assets/features/admin/templates/namespace/create.html',
            controller: 'AdminNamespaceCreateController'
          })

          .state('admin.namespace.detail', {
            url: '/detail/:nsadmin',
            templateUrl: '/assets/features/admin/templates/namespace.html'
          })

            .state('admin.namespace.detail.metadata', {
              url: '/metadata',
              templateUrl: '/assets/features/admin/templates/namespace/metadata.html',
              controller: 'AdminNamespaceMetadataController'
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
              controller: 'AdminDatatypesController'
            })

            .state('admin.namespace.detail.datasets', {
              url: '/datasets',
              templateUrl: '/assets/features/admin/templates/namespace/datasets.html',
              controller: 'AdminDatasetsController'
            })

            .state('admin.namespace.detail.apps', {
              url: '/apps',
              templateUrl: '/assets/features/admin/templates/namespace/apps.html',
              controller: 'AdminNamespaceAppController'
            })
              .state('admin.namespace.detail.apps.metadata', {
                parent: 'admin.namespace.detail',
                url: '/:appId',
                templateUrl: '/assets/features/admin/templates/namespace/app-metadata.html',
                controller: 'AdminNamespaceAppMetadataController'
              });
  });
