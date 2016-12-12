/*
 * Copyright © 2015-2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

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
        templateUrl: '/old_assets/features/admin/templates/admin.html',
        controller: 'AdminController'
      })

        .state('admin.overview', {
          url: '',
          templateUrl: '/old_assets/features/admin/templates/overview.html',
          controller: 'OverviewController',
          ncyBreadcrumb: {
            label: 'Management'
          }
        })

        .state('admin.system', {
          abstract: true,
          url: '/system',
          template: '<ui-view/>'
        })
          .state('admin.system.overview', {
            url: '',
            templateUrl: '/old_assets/features/admin/templates/system.html'
          })
          .state('admin.system.configuration', {
            url: '/configuration',
            templateUrl: '/old_assets/features/admin/templates/system/configuration.html',
            controller: 'SystemConfigurationController',
            ncyBreadcrumb: {
              label: 'Configuration',
              parent: 'admin.overview'
            }
          })

          .state('admin.system.services', {
            url: '/services',
            templateUrl: '/old_assets/features/admin/templates/system/services.html',
            controller: 'SystemServicesController',
            ncyBreadcrumb: {
              label: 'Services',
              parent: 'admin.overview'
            }
          })
            .state('admin.system.services.detail', {
              parent: 'admin.system',
              url: '/services/detail/:serviceName',
              templateUrl: '/old_assets/features/admin/templates/system/service-detail.html',
              controller: 'SystemServiceDetailController'
            })
              .state('admin.system.services.detail.metadata', {
                url: '/metadata',
                templateUrl: '/old_assets/features/admin/templates/partials/service-detail-metadata.html',
                ncyBreadcrumb: {
                  label: 'Metadata',
                  parent: 'admin.system.services'
                }
              })
              .state('admin.system.services.detail.logs', {
                url: '/logs',
                templateUrl: '/old_assets/features/admin/templates/partials/service-detail-log.html',
                controller: 'SystemServiceLogController',
                ncyBreadcrumb: {
                  label: 'Logs',
                  parent: 'admin.system.services'
                }
              })

          .state('admin.system.preferences', {
            url: '/preferences',
            templateUrl: '/old_assets/features/admin/templates/preferences.html',
            controller: 'PreferencesController',
            ncyBreadcrumb: {
              label: 'Preferences',
              parent: 'admin.overview'
            },
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
            onEnter: function($uibModal, $state) {
              $uibModal.open({
                templateUrl: '/old_assets/features/admin/templates/namespace/create.html',
                size: 'lg',
                backdrop: true,
                keyboard: true,
                controller: 'NamespaceCreateController'
              }).result.finally(function() {
                $state.go('admin.overview', {}, { reload: true });
              });
            },
            onExit: function($uibModalStack) {
              $uibModalStack.dismissAll();
            },
            ncyBreadcrumb: {
              skip: true
            }
          })

          .state('admin.namespace.detail', {
            url: '/detail/:nsadmin',
            templateUrl: '/old_assets/features/admin/templates/namespace.html'
          })
            .state('admin.namespace.detail.preferences', {
              url: '/preferences',
              templateUrl: '/old_assets/features/admin/templates/preferences.html',
              controller: 'PreferencesController',
              resolve: {
                rSource: function () {
                  return 'NAMESPACE';
                }
              },
              ncyBreadcrumb: {
                label: 'Preferences',
                parent: 'admin.namespace.detail.settings'
              }
            })

            .state('admin.namespace.detail.templateslist', {
              url: '/templates',
              templateUrl: '/old_assets/features/admin/templates/namespace/templates-list.html',
              controller: 'NamespaceTemplatesListController',
              controllerAs: 'TemplatesListController',
              ncyBreadcrumb: {
                label: 'Templates',
                parent: 'admin.namespace.detail.settings'
              }
            })

            .state('admin.namespace.detail.templates', {
              url: '/templates/create',
              templateUrl: '/old_assets/features/admin/templates/namespace/templates.html',
              controller: 'NamespaceTemplatesController',
              controllerAs: 'TemplatesController',
              ncyBreadcrumb: {
                label: 'Create',
                parent: 'admin.namespace.detail.templateslist'
              }
            })

            .state('admin.namespace.detail.templateedit', {
              url: '/templates/edit/:templateType/:pluginType/:pluginTemplate',
              templateUrl: '/old_assets/features/admin/templates/namespace/templates.html',
              controller: 'NamespaceTemplatesController',
              controllerAs: 'TemplatesController',
              ncyBreadcrumb: {
                label: '{{$state.params.pluginTemplate}}',
                parent: 'admin.namespace.detail.templateslist'
              }
            })

            .state('admin.namespace.detail.settings', {
              url: '/settings',
              templateUrl: '/old_assets/features/admin/templates/namespace/settings.html',
              controller: 'NamespaceSettingsController',
              ncyBreadcrumb: {
                label: '{{$state.params.nsadmin}}',
                parent: 'admin.overview'
              }
            })

            .state('admin.namespace.detail.data', {
              url: '/data',
              templateUrl: '/old_assets/features/admin/templates/namespace/datasets.html',
              controller: 'NamespaceDatasetsController',
              ncyBreadcrumb: {
                label: 'Datasets',
                parent: 'admin.namespace.detail.settings'
              }
            })
              .state('admin.namespace.detail.data.datasetmetadata', {
                url: '/datasets/:datasetId',
                controller: 'NamespaceDatasetMetadataController',
                templateUrl: '/old_assets/features/admin/templates/namespace/dataset-metadata.html',
                ncyBreadcrumb: {
                  label: '{{$state.params.datasetId}}',
                  parent: 'admin.namespace.detail.data'
                }
              })

              .state('admin.namespace.detail.data.streamcreate', {
                url:'/streams/create',
                onEnter: function($uibModal) {
                  $uibModal.open({
                    templateUrl: '/old_assets/features/admin/templates/namespace/streamscreate.html',
                    size: 'lg',
                    backdrop: true,
                    keyboard: true,
                    controller: 'NamespaceStreamsCreateController'
                  });
                },
                onExit: function($uibModalStack) {
                  $uibModalStack.dismissAll();
                },
                ncyBreadcrumb: {
                  skip: true
                }
              })

              .state('admin.namespace.detail.data.streammetadata', {
                url: '/streams/detail/:streamId',
                controller: 'NamespaceStreamMetadataController',
                templateUrl: '/old_assets/features/admin/templates/namespace/stream-metadata.html',
                ncyBreadcrumb: {
                  label: '{{$state.params.streamId}}',
                  parent: 'admin.namespace.detail.data'
                }
              })

            .state('admin.namespace.detail.apps', {
              url: '/apps',
              templateUrl: '/old_assets/features/admin/templates/namespace/apps.html',
              controller: 'NamespaceAppController',
              ncyBreadcrumb: {
                label: 'Applications',
                parent: 'admin.namespace.detail.settings'
              }
            })
              .state('admin.namespace.detail.apps.metadata', {
                parent: 'admin.namespace.detail',
                url: '/:appId',
                templateUrl: '/old_assets/features/admin/templates/namespace/app-metadata.html',
                controller: 'NamespaceAppMetadataController',
                ncyBreadcrumb: {
                  label: 'Metadata',
                  parent: 'admin.namespace.detail.apps'
                }
              })
                .state('admin.namespace.detail.apps.metadata.preference', {
                  url: '/preferences',
                  templateUrl: '/old_assets/features/admin/templates/preferences.html',
                  controller: 'PreferencesController',
                  ncyBreadcrumb: {
                    label: 'Preferences',
                    parent: 'admin.namespace.detail.settings'
                  },
                  resolve: {
                    rSource: function () {
                      return 'APPLICATION';
                    }
                  }
                });

  });
