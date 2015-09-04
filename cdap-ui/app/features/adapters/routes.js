/*
 * Copyright © 2015 Cask Data, Inc.
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

angular.module(PKG.name + '.feature.adapters')
  .config(function($stateProvider, $urlRouterProvider, MYAUTH_ROLE) {
    $stateProvider
      .state('adapters', {
        url: '/hydrator',
        abstract: true,
        parent: 'ns',
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'development'
        },
        template: '<ui-view/>'
      })

        .state('adapters.list', {
          url: '',
          templateUrl: '/assets/features/adapters/templates/list.html',
          controller: 'AdaptersListController',
          controllerAs: 'ListController'
        })

        .state('adapters.drafts', {
          url: '/drafts',
          templateUrl: '/assets/features/adapters/templates/drafts.html',
          controller: 'AdapterDraftsController',
          ncyBreadcrumb: {
            label: 'All Drafts',
            parent: 'overview'
          }
        })

        .state('adapters.create', {
          url: '/create?name&type',
          params: {
            data: null
          },
          resolve: {
            rConfig: function($stateParams, mySettings, $q) {
              var defer = $q.defer();
              if ($stateParams.name) {
                mySettings.get('adapterDrafts')
                  .then(function(res) {
                    var draft = res[$stateParams.name];
                    if (angular.isObject(draft)) {
                      draft.name = $stateParams.name;
                      defer.resolve(draft);
                    } else {
                      defer.resolve(false);
                    }
                  });
              } else if ($stateParams.data){
                defer.resolve($stateParams.data);
              } else {
                defer.resolve(false);
              }
              return defer.promise;
            }
          },
          // controller: 'AdapterCreateController as AdapterCreateController',
          views: {
            '': {
              templateUrl: '/assets/features/adapters/templates/create.html',
              controller: 'AdapterCreateController as AdapterCreateController'
            },
            'metadata@adapters.create': {
              templateUrl: '/assets/features/adapters/templates/create/metadata.html',
              controller: 'MetadataController as MetadataController'
            },
            'canvas@adapters.create': {
              templateUrl: '/assets/features/adapters/templates/create/canvas.html'
            },
            'leftpanel@adapters.create': {
              templateUrl: '/assets/features/adapters/templates/create/leftpanel.html',
              controller: 'LeftPanelController as LeftPanelController'
            },
            'toppanel@adapters.create': {
              templateUrl: '/assets/features/adapters/templates/create/toppanel.html',
              controller: 'TopPanelController as TopPanelController'
            },
            'bottompanel@adapters.create': {
              templateUrl: '/assets/features/adapters/templates/create/bottompanel.html',
              controller: 'BottomPanelController as BottomPanelController'
            }
          },
          ncyBreadcrumb: {
            skip: true
          }
        })

        .state('adapters.detail', {
          url: '/:adapterId',
          data: {
            authorizedRoles: MYAUTH_ROLE.all,
            highlightTab: 'development'
          },
          resolve : {
            rRuns: function($stateParams, $q, myAdapterApi) {
              var defer = $q.defer();
              // Using _cdapPath here as $state.params is not updated with
              // runid param when the request goes out
              // (timing issue with re-direct from login state).
              var params = {
                namespace: $stateParams.namespace,
                adapter: $stateParams.adapterId
              };

              myAdapterApi.runs(params)
                .$promise
                .then(function(res) {
                  defer.resolve(res);
                });
              return defer.promise;
            },
            rAdapterDetail: function($stateParams, $q, myAdapterApi) {
              var params = {
                namespace: $stateParams.namespace,
                adapter: $stateParams.adapterId
              };

              return myAdapterApi.get(params).$promise;
            }
          },
          ncyBreadcrumb: {
            parent: 'apps.list',
            label: '{{$state.params.adapterId}}'
          },
          templateUrl: '/assets/features/adapters/templates/detail.html',
          controller: 'AdpaterDetailController'
        })
          .state('adapters.detail.runs',{
            url: '/runs',
            templateUrl: '/assets/features/adapters/templates/tabs/runs.html',
            controller: 'AdapterRunsController',
            ncyBreadcrumb: {
              parent: 'apps.list',
              label: '{{$state.params.adapterId}}'
            }
          })
            .state('adapters.detail.runs.run', {
              url: '/:runid',
              templateUrl: '/assets/features/adapters/templates/tabs/runs/run-detail.html',
              ncyBreadcrumb: {
                label: '{{$state.params.runid}}'
              }
            })

        .state('adapters.detail.datasets', {
          url: '/datasets',
          data: {
            authorizedRoles: MYAUTH_ROLE.all,
            highlightTab: 'development'
          },
          templateUrl: 'data-list/data-list.html',
          controller: 'AdapterDatasetsController',
          ncyBreadcrumb: {
            label: 'Datasets',
            parent: 'adapters.detail.runs'
          }
        })
        .state('adapters.detail.history', {
          url: '/history',
          data: {
            authorizedRoles: MYAUTH_ROLE.all,
            highlightTab: 'development'
          },
          templateUrl: '/assets/features/adapters/templates/tabs/history.html',
          controller: 'AdapterRunsController',
          ncyBreadcrumb: {
            label: 'History',
            parent: 'adapters.detail.runs'
          }
        })
        .state('adapters.detail.schedule', {
          url: '/schedule',
          data: {
            authorizedRoles: MYAUTH_ROLE.all,
            highlightTab: 'development'
          },
          templateUrl: '/assets/features/adapters/templates/tabs/schedule.html',
          controller: 'ScheduleController',
          controllerAs: 'ScheduleController',
          ncyBreadcrumb: {
            label: 'Schedule',
            parent: 'adapters.detail.runs'
          }
        });
  });
