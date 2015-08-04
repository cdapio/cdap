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
          controller: 'OverviewController'
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
          .state('admin.system.configuration', {
            url: '/configuration',
            templateUrl: '/assets/features/admin/templates/system/configuration.html',
            controller: 'SystemConfigurationController'
          })

          .state('admin.system.services', {
            url: '/services',
            templateUrl: '/assets/features/admin/templates/system/services.html',
            controller: 'SystemServicesController'
          })
            .state('admin.system.services.detail', {
              parent: 'admin.system',
              url: '/services/detail/:serviceName',
              templateUrl: '/assets/features/admin/templates/system/service-detail.html',
              controller: 'SystemServiceDetailController'
            })
              .state('admin.system.services.detail.metadata', {
                url: '/metadata',
                templateUrl: '/assets/features/admin/templates/partials/service-detail-metadata.html'
              })
              .state('admin.system.services.detail.logs', {
                url: '/logs',
                templateUrl: '/assets/features/admin/templates/partials/service-detail-log.html',
                controller: 'SystemServiceLogController'
              })

          .state('admin.system.preferences', {
            url: '/preferences',
            templateUrl: '/assets/features/admin/templates/preferences.html',
            controller: 'PreferencesController',
            resolve: {
              rSource: function() {
                return 'SYSTEM';
              }
            }
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
            onEnter: function($bootstrapModal, $state, myNamespace) {
              $bootstrapModal.open({
                templateUrl: '/assets/features/admin/templates/namespace/create.html',
                size: 'lg',
                backdrop: true,
                keyboard: true,
                controller: 'NamespaceCreateController'
              }).result.finally(function() {
                myNamespace.getList(true).then(function() {
                  $state.go('admin.overview', {}, {reload: true});
                });

              });
            },
            onExit: function($modalStack) {
              $modalStack.dismissAll();
            }
          })

          .state('admin.namespace.detail', {
            url: '/detail/:nsadmin',
            templateUrl: '/assets/features/admin/templates/namespace.html'
          })
            .state('admin.namespace.detail.preferences', {
              url: '/preferences',
              templateUrl: '/assets/features/admin/templates/preferences.html',
              controller: 'PreferencesController',
              resolve: {
                rSource: function () {
                  return 'NAMESPACE';
                }
              }
            })

            .state('admin.namespace.detail.metadata', {
              url: '/metadata',
              templateUrl: '/assets/features/admin/templates/namespace/metadata.html',
              controller: 'NamespaceMetadataController'
            })

            .state('admin.namespace.detail.settings', {
              url: '/settings',
              templateUrl: '/assets/features/admin/templates/namespace/settings.html',
              controller: 'NamespaceSettingsController'
            })

            .state('admin.namespace.detail.data', {
              url: '/data',
              templateUrl: '/assets/features/admin/templates/namespace/datasets.html',
              controller: 'NamespaceDatasetsController'
            })
              .state('admin.namespace.detail.data.datasetmetadata', {
                url: '/datasets/:datasetId',
                controller: 'NamespaceDatasetMetadataController',
                templateUrl: '/assets/features/admin/templates/namespace/dataset-metadata.html'
              })

              .state('admin.namespace.detail.data.streamcreate', {
                url:'/streams/create',
                onEnter: function($bootstrapModal, $state) {
                  $bootstrapModal.open({
                    templateUrl: '/assets/features/admin/templates/namespace/streamscreate.html',
                    size: 'lg',
                    backdrop: true,
                    keyboard: true,
                    controller: 'NamespaceStreamsCreateController'
                  }).result.finally(function() {
                    $state.go('admin.namespace.detail.data', {}, { reload: true });
                  });
                },
                onExit: function($modalStack) {
                  $modalStack.dismissAll();
                }
              })

              .state('admin.namespace.detail.data.streammetadata', {
                url: '/streams/detail/:streamId',
                controller: 'NamespaceStreamMetadataController',
                templateUrl: '/assets/features/admin/templates/namespace/stream-metadata.html'
              })

            .state('admin.namespace.detail.apps', {
              url: '/apps',
              templateUrl: '/assets/features/admin/templates/namespace/apps.html',
              controller: 'NamespaceAppController'
            })
              .state('admin.namespace.detail.apps.metadata', {
                parent: 'admin.namespace.detail',
                url: '/:appId',
                templateUrl: '/assets/features/admin/templates/namespace/app-metadata.html',
                controller: 'NamespaceAppMetadataController'
              })
                .state('admin.namespace.detail.apps.metadata.preference', {
                  url: '/preferences',
                  templateUrl: '/assets/features/admin/templates/preferences.html',
                  controller: 'PreferencesController',
                  resolve: {
                    rSource: function () {
                      return 'APPLICATION';
                    }
                  }
                });

  });
