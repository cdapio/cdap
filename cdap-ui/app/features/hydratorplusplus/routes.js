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

angular.module(PKG.name + '.feature.hydratorplusplus')
  .config(function($stateProvider, $urlRouterProvider, MYAUTH_ROLE) {
    $stateProvider
      .state('hydratorplusplus', {
        url: '/hydratorplusplus',
        abstract: true,
        parent: 'ns',
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'hydratorList'
        },
        template: '<ui-view/>'
      })

        .state('hydratorplusplus.create', {
          url: '/studio?draftId&artifactType',
          params: {
            data: null
          },
          data: {
            authorizedRoles: MYAUTH_ROLE.all,
            highlightTab: 'hydratorStudioPlusPlus'
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
            rSelectedArtifact: function($stateParams, $q, myPipelineApi) {
              var defer = $q.defer();
              myPipelineApi.fetchArtifacts({
                namespace: $stateParams.namespace
              }).$promise.then((res) => {
                let uiSupportedArtifacts = ['cdap-etl-batch', 'cdap-etl-realtime', 'cdap-etl-data-pipeline'];
                let filteredRes;
                if ($stateParams.artifactType) {
                  filteredRes = res.filter( r => r.name === $stateParams.artifactType );
                  if (filteredRes.length) {
                    defer.resolve(filteredRes[0].name);
                  } else {
                    defer.resolve(res[0].name);
                  }
                } else {
                  filteredRes = res.filter( r => uiSupportedArtifacts.indexOf(r.name) !== -1 );
                  $stateParams.artifactType = filteredRes.filter(fres => fres.name === 'cdap-etl-data-pipeline')[0].name;
                  defer.resolve($stateParams.artifactType);
                }
              });
              return defer.promise;
            },
            rArtifacts: function(myPipelineApi, $stateParams, $q, HydratorPlusPlusOrderingFactory) {
              var defer = $q.defer();
              myPipelineApi.fetchArtifacts({
                namespace: $stateParams.namespace
              }).$promise
              .then((res) => {
                let uiSupportedArtifacts = ['cdap-etl-batch', 'cdap-etl-realtime', 'cdap-etl-data-pipeline'];
                let filteredRes = res.filter( r => uiSupportedArtifacts.indexOf(r.name) !== -1 );

                filteredRes = filteredRes.map( r => {
                  r.label = HydratorPlusPlusOrderingFactory.getArtifactDisplayName(r.name);
                  return r;
                });
                defer.resolve(filteredRes);
              });
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
              templateUrl: '/assets/features/hydratorplusplus/templates/create/studio.html',
              controller: 'HydratorPlusPlusStudioCtrl as HydratorPlusPlusStudioCtrl'
            },
            'canvas@hydratorplusplus.create': {
              templateUrl: '/assets/features/hydratorplusplus/templates/create/canvas.html',
              controller: 'HydratorPlusPlusCreateCanvasCtrl',
              controllerAs: 'CanvasCtrl'
            },
            'leftpanel@hydratorplusplus.create': {
              templateUrl: '/assets/features/hydratorplusplus/templates/create/leftpanel.html',
              controller: 'HydratorPlusPlusLeftPanelCtrl as HydratorPlusPlusLeftPanelCtrl'
            },
            'toppanel@hydratorplusplus.create': {
              templateUrl: '/assets/features/hydratorplusplus/templates/create/toppanel.html',
              controller: 'HydratorPlusPlusTopPanelCtrl as HydratorPlusPlusTopPanelCtrl'
            },
            'bottompanel@hydratorplusplus.create': {
              templateUrl: '/assets/features/hydratorplusplus/templates/create/bottompanel.html',
              controller: 'HydratorPlusPlusBottomPanelCtrl as HydratorPlusPlusBottomPanelCtrl'
            }
          },
          onExit: function($uibModalStack) {
            $uibModalStack.dismissAll();
          }
        })

        .state('hydratorplusplus.detail', {
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
              templateUrl: '/assets/features/hydratorplusplus/templates/detail.html',
              controller: 'HydratorPlusPlusDetailCtrl'
            },
            'toppanel@hydratorplusplus.detail': {
              templateUrl: '/assets/features/hydratorplusplus/templates/detail/top-panel.html',
              controller: 'HydratorDetailTopPanelController',
              controllerAs: 'TopPanelCtrl'
            },
            'bottompanel@hydratorplusplus.detail': {
              templateUrl: '/assets/features/hydratorplusplus/templates/detail/bottom-panel.html',
              controller: 'HydratorPlusPlusDetailBottomPanelCtrl',
              controllerAs: 'BottomPanelCtrl'
            },
            'canvas@hydratorplusplus.detail': {
              templateUrl: '/assets/features/hydratorplusplus/templates/detail/canvas.html',
              controller: 'HydratorPlusPlusDetailCanvasCtrl',
              controllerAs: 'CanvasCtrl'
            }
          }
        });

  });
