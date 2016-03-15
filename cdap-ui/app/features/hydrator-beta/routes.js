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

angular.module(PKG.name + '.feature.hydrator-beta')
  .config(function($stateProvider, $urlRouterProvider, MYAUTH_ROLE) {
    $stateProvider
      .state('hydrator-beta', {
        url: '/hydrator-beta',
        abstract: true,
        parent: 'ns',
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'hydratorList'
        },
        template: '<ui-view/>'
      })

        .state('hydrator-beta.list', {
          url: '',
          data: {
            authorizedRoles: MYAUTH_ROLE.all,
            highlightTab: 'hydratorList'
          },
          templateUrl: '/assets/features/hydrator-beta/templates/list.html',
          controller: 'HydratorListController',
          controllerAs: 'ListController'
        })


        .state('hydrator-beta.create', {
          url: '/studio?draftId',
          params: {
            data: null
          },
          data: {
            authorizedRoles: MYAUTH_ROLE.all,
            highlightTab: 'hydratorStudioBeta'
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
              templateUrl: '/assets/features/hydrator-beta/templates/create/studio.html',
              controller: 'HydratorCreateStudioController as HydratorCreateStudioController'
            },
            'canvas@hydrator-beta.create': {
              templateUrl: '/assets/features/hydrator-beta/templates/create/canvas.html',
              controller: 'HydratorCreateCanvasController',
              controllerAs: 'CanvasCtrl'
            },
            'leftpanel@hydrator-beta.create': {
              templateUrl: '/assets/features/hydrator-beta/templates/create/leftpanel.html',
              controller: 'LeftPanelController as LeftPanelController'
            },
            'toppanel@hydrator-beta.create': {
              templateUrl: '/assets/features/hydrator-beta/templates/create/toppanel.html',
              controller: 'TopPanelController as TopPanelController'
            },
            'bottompanel@hydrator-beta.create': {
              templateUrl: '/assets/features/hydrator-beta/templates/create/bottompanel.html',
              controller: 'BottomPanelController as BottomPanelController'
            }
          },
          onExit: function($uibModalStack) {
            $uibModalStack.dismissAll();
          }
        })

        .state('hydrator-beta.detail', {
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
              templateUrl: '/assets/features/hydrator-beta/templates/detail.html',
              controller: 'HydratorDetailController'
            },
            'toppanel@hydrator-beta.detail': {
              templateUrl: '/assets/features/hydrator-beta/templates/detail/top-panel.html',
              controller: 'HydratorDetailTopPanelController',
              controllerAs: 'TopPanelCtrl'
            },
            'bottompanel@hydrator-beta.detail': {
              templateUrl: '/assets/features/hydrator-beta/templates/detail/bottom-panel.html',
              controller: 'HydratorDetailBottomPanelController',
              controllerAs: 'BottomPanelCtrl'
            },
            'canvas@hydrator-beta.detail': {
              templateUrl: '/assets/features/hydrator-beta/templates/detail/canvas.html',
              controller: 'HydratorDetailCanvasController',
              controllerAs: 'CanvasCtrl'
            }
          }
        });

  });
