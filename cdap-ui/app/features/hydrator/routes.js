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

angular.module(PKG.name + '.feature.hydrator')
  .config(function($stateProvider, $urlRouterProvider, MYAUTH_ROLE) {
    $stateProvider
      .state('hydrator', {
        url: '/hydrator',
        abstract: true,
        parent: 'ns',
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'hydratorList'
        },
        template: '<ui-view/>'
      })

        .state('hydrator.list', {
          url: '',
          data: {
            authorizedRoles: MYAUTH_ROLE.all,
            highlightTab: 'hydratorList'
          },
          templateUrl: '/assets/features/hydrator/templates/list.html',
          controller: 'HydratorListController',
          controllerAs: 'ListController'
        })

        .state('hydrator.create', {
          url: '/create',
          data: {
            authorizedRoles: MYAUTH_ROLE.all,
            highlightTab: 'hydratorStudio'
          },
          templateUrl: '/assets/features/hydrator/templates/create.html',
          controller: 'HydratorCreateController',
          controllerAs: 'HydratorCreateController',
          ncyBreadcrumb: {
            skip: true
          }
        })
          .state('hydrator.create.studio', {
            url: '/studio?draftId&type',
            params: {
              data: null
            },
            resolve: {
              rConfig: function($stateParams, mySettings, $q, myHelpers) {
                var defer = $q.defer();
                if ($stateParams.draftId) {
                  mySettings.get('hydratorDrafts', true)
                    .then(function(res) {
                      var draft = myHelpers.objectQuery(res, $stateParams.namespace, $stateParams.draftId);
                      if (angular.isObject(draft)) {
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
              rVersion: function($state, MyCDAPDataSource) {
                var dataSource = new MyCDAPDataSource();
                return dataSource.request({
                  _cdapPath: '/version'
                });
              }
            },
            views: {
              '': {
                templateUrl: '/assets/features/hydrator/templates/create/studio.html',
                controller: 'HydratorCreateStudioController as HydratorCreateStudioController'
              },
              'canvas@hydrator.create.studio': {
                templateUrl: '/assets/features/hydrator/templates/create/canvas.html',
                controller: 'HydratorCreateCanvasController',
                controllerAs: 'CanvasCtrl'
              },
              'leftpanel@hydrator.create.studio': {
                templateUrl: '/assets/features/hydrator/templates/create/leftpanel.html',
                controller: 'LeftPanelController as LeftPanelController'
              },
              'toppanel@hydrator.create.studio': {
                templateUrl: '/assets/features/hydrator/templates/create/toppanel.html',
                controller: 'TopPanelController as TopPanelController'
              },
              'bottompanel@hydrator.create.studio': {
                templateUrl: '/assets/features/hydrator/templates/create/bottompanel.html',
                controller: 'BottomPanelController as BottomPanelController'
              }
            },
            onExit: function($modalStack) {
              $modalStack.dismissAll();
            }
          })

        .state('hydrator.detail', {
          url: '/view/:pipelineId',
          data: {
            authorizedRoles: MYAUTH_ROLE.all,
            highlightTab: 'hydratorList'
          },
          resolve : {
            rPipelineDetail: function($stateParams, $q, myPipelineApi) {
              var params = {
                namespace: $stateParams.namespace,
                pipeline: $stateParams.pipelineId
              };

              return myPipelineApi.get(params).$promise;
            }
          },
          ncyBreadcrumb: {
            parent: 'apps.list',
            label: '{{$state.params.pipelineId}}'
          },
          views: {
            '': {
              templateUrl: '/assets/features/hydrator/templates/detail.html',
              controller: 'HydratorDetailController'
            },
            'toppanel@hydrator.detail': {
              templateUrl: '/assets/features/hydrator/templates/detail/top-panel.html',
              controller: 'HydratorDetailTopPanelController',
              controllerAs: 'TopPanelCtrl'
            },
            'bottompanel@hydrator.detail': {
              templateUrl: '/assets/features/hydrator/templates/detail/bottom-panel.html',
              controller: 'HydratorDetailBottomPanelController',
              controllerAs: 'BottomPanelCtrl'
            },
            'canvas@hydrator.detail': {
              templateUrl: '/assets/features/hydrator/templates/detail/canvas.html',
              controller: 'HydratorDetailCanvasController',
              controllerAs: 'CanvasCtrl'
            }
          }
        });

  });
