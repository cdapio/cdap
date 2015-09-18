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

        .state('adapters.create', {
          url: '/create',
          templateUrl: '/assets/features/adapters/templates/create.html',
          controller: 'AdapterCreateController',
          controllerAs: 'AdapterCreateController',
          ncyBreadcrumb: {
            skip: true
          }
        })
          .state('adapters.create.studio', {
            url: '/studio?name&type',
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
              },
              rVersion: function($state, MyDataSource) {
                var dataSource = new MyDataSource();
                return dataSource.request({
                  _cdapPath: '/version'
                });
              }
            },
            views: {
              '': {
                templateUrl: '/assets/features/adapters/templates/create/studio.html',
                controller: 'AdapterCreateStudioController as AdapterCreateStudioController'
              },
              'canvas@adapters.create.studio': {
                templateUrl: '/assets/features/adapters/templates/create/canvas.html'
              },
              'leftpanel@adapters.create.studio': {
                templateUrl: '/assets/features/adapters/templates/create/leftpanel.html',
                controller: 'LeftPanelController as LeftPanelController'
              },
              'toppanel@adapters.create.studio': {
                templateUrl: '/assets/features/adapters/templates/create/toppanel.html',
                controller: 'TopPanelController as TopPanelController'
              },
              'bottompanel@adapters.create.studio': {
                templateUrl: '/assets/features/adapters/templates/create/bottompanel.html',
                controller: 'BottomPanelController as BottomPanelController'
              }
            },
          })

        .state('adapters.detail', {
          url: '/view/:adapterId',
          data: {
            authorizedRoles: MYAUTH_ROLE.all,
            highlightTab: 'development'
          },
          resolve : {
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
        });

  });
