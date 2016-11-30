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

angular.module(PKG.name + '.feature.hydrator')
  .config(function($stateProvider, $urlRouterProvider, MYAUTH_ROLE) {
    $stateProvider
      .state('home', {
        url: '/',
        template: '<ui-view/>',
        resolve: {
          rNsList: function (myNamespace) {
            return myNamespace.getList();
          }
        },
        controller: 'HydratorHomeController'
      })
      .state('hydrator', {
        url: '/ns/:namespace',
        abstract: true,
        template: '<ui-view/>',
        title: 'Hydrator',
        resolve: {
          // This is f%&$*d up. We need to cause this manual delay for react to finish its click handlers
          // before angular takes up the state change routing -_-.
          rDelay: function($q) {
            var defer = $q.defer();
            setTimeout(() => {
              defer.resolve();
            });
            return defer.promise;
          }
        },
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'development'
        }
      })
        .state('hydrator.create', {
          url: '/studio?artifactType&draftId',
          onEnter: function() {
            document.title = 'Hydrator Studio';
          },
          params: {
            data: null,
            isClone: null
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
            rSelectedArtifact: function($stateParams, $q, myPipelineApi, myAlertOnValium, $state, GLOBALS, $rootScope) {
              var defer = $q.defer();
              let uiSupportedArtifacts = [GLOBALS.etlBatch, GLOBALS.etlRealtime, GLOBALS.etlDataPipeline, GLOBALS.etlDataStreams];
              let isArtifactValid = (backendArtifacts, artifact) => {
                return backendArtifacts.filter( a =>
                  (a.name === artifact && a.version === $rootScope.cdapVersion)
                ).length;
              };
              let isAnyUISupportedArtifactPresent = (backendArtifacts) => {
                return backendArtifacts
                        .filter( artifact => artifact.version === $rootScope.cdapVersion)
                        .filter( artifact => uiSupportedArtifacts.indexOf(artifact.name) !== -1);
              };
              let getValidUISupportedArtifact = (backendArtifacts) => {
                let validUISupportedArtifact = isAnyUISupportedArtifactPresent(backendArtifacts);
                return validUISupportedArtifact.length ?  validUISupportedArtifact[0]: false;
              };

              let showError = (message) => {
                message = (typeof message === 'string' ? message : GLOBALS.en.hydrator.studio.error['MISSING-SYSTEM-ARTIFACTS']);
                myAlertOnValium.show({
                  type: 'danger',
                  content: message
                });
              };

              myPipelineApi.fetchArtifacts({
                namespace: $stateParams.namespace
              }).$promise.then((artifactsFromBackend) => {
                let showWarningAndNavigateAway = () => {
                  if (!$state.current.name.length) {
                    $state.go('hydrator.list').then(showError);
                    return;
                  } else {
                    $state.go($state.current).then(showError);
                  }
                };

                let chooseDefaultArtifact = () => {
                  if(!isArtifactValid(artifactsFromBackend, GLOBALS.etlDataPipeline)) {
                    if (!isAnyUISupportedArtifactPresent(artifactsFromBackend).length) {
                      return showWarningAndNavigateAway();
                    } else {
                      $stateParams.artifactType = getValidUISupportedArtifact(artifactsFromBackend).name;
                      defer.resolve($stateParams.artifactType);
                    }
                  } else {
                    $stateParams.artifactType = GLOBALS.etlDataPipeline;
                    defer.resolve($stateParams.artifactType);
                  }
                };

                if(!artifactsFromBackend.length) {
                  return showWarningAndNavigateAway();
                }

                if(!isArtifactValid(artifactsFromBackend, $stateParams.artifactType)) {
                  chooseDefaultArtifact();
                } else {
                  defer.resolve($stateParams.artifactType);
                }
              },
              (err) => {
                showError(err);
              }
            );
              return defer.promise;
            },
            rArtifacts: function(myPipelineApi, $stateParams, $q, HydratorPlusPlusOrderingFactory, GLOBALS, $rootScope) {
              var defer = $q.defer();
              myPipelineApi.fetchArtifacts({
                namespace: $stateParams.namespace
              }).$promise
              .then((res) => {
                if (!res.length) {
                  return;
                } else {
                  let uiSupportedArtifacts = [GLOBALS.etlBatch, GLOBALS.etlRealtime, GLOBALS.etlDataPipeline, GLOBALS.etlDataStreams];
                  let filteredRes = res
                    .filter( artifact => artifact.version === $rootScope.cdapVersion)
                    .filter( r => uiSupportedArtifacts.indexOf(r.name) !== -1 );

                  filteredRes = filteredRes.map( r => {
                    r.label = HydratorPlusPlusOrderingFactory.getArtifactDisplayName(r.name);
                    return r;
                  });
                  defer.resolve(filteredRes);
                }
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
              templateUrl: '/assets/features/hydrator/templates/create/studio.html',
              controller: 'HydratorPlusPlusStudioCtrl as HydratorPlusPlusStudioCtrl'
            },
            'canvas@hydrator.create': {
              templateUrl: '/assets/features/hydrator/templates/create/canvas.html',
              controller: 'HydratorPlusPlusCreateCanvasCtrl',
              controllerAs: 'CanvasCtrl'
            },
            'leftpanel@hydrator.create': {
              templateUrl: '/assets/features/hydrator/templates/create/leftpanel.html',
              controller: 'HydratorPlusPlusLeftPanelCtrl as HydratorPlusPlusLeftPanelCtrl'
            },
            'toppanel@hydrator.create': {
              templateUrl: '/assets/features/hydrator/templates/create/toppanel.html',
              controller: 'HydratorPlusPlusTopPanelCtrl as HydratorPlusPlusTopPanelCtrl'
            }
          },
          onExit: function($uibModalStack) {
            $uibModalStack.dismissAll();
          }
        })

        .state('hydrator.detail', {
          url: '/view/:pipelineId',
          data: {
            authorizedRoles: MYAUTH_ROLE.all,
            highlightTab: 'hydratorList'
          },
          onEnter: function($stateParams) {
            document.title = 'Pipeline - ' + $stateParams.pipelineId;
          },
          resolve : {
            rPipelineDetail: function($stateParams, $q, myPipelineApi, myAlertOnValium, $state) {
              var params = {
                namespace: $stateParams.namespace,
                pipeline: $stateParams.pipelineId
              };

              return myPipelineApi
                .get(params)
                .$promise
                .then(
                  (pipelineDetail) => {
                    let config = pipelineDetail.configuration;
                    try {
                      config = JSON.parse(config);
                    } catch(e) {
                      myAlertOnValium.show({
                        type: 'danger',
                        content: 'Invalid configuration JSON.'
                      });
                      $q.reject(false);
                      // FIXME: We should not have done this. But ui-router when rejected on a 'resolve:' function takes it to the parent state apparently
                      // and in our case the parent state is 'hydrator and since its an abstract state it goes to home.'
                      $state.go('hydrator.list');
                      return;
                    }
                    if(!config.stages) {
                      myAlertOnValium.show({
                        type: 'danger',
                        content: 'Pipeline is created using older version of hydrator. Please upgrage the pipeline to newer version(3.4) to view in UI.'
                      });
                      $q.reject(false);
                      // FIXME: We should not have done this. But ui-router when rejected on a 'resolve:' function takes it to the parent state apparently
                      // and in our case the parent state is 'hydrator and since its an abstract state it goes to home.'
                      $state.go('hydrator.list');
                      return;
                    }
                    return $q.resolve(pipelineDetail);
                  }
                );
            }
          },
          ncyBreadcrumb: {
            parent: 'apps.list',
            label: '{{$state.params.pipelineId}}'
          },
          views: {
            '': {
              templateUrl: '/assets/features/hydrator/templates/detail.html',
              controller: 'HydratorPlusPlusDetailCtrl'
            },
            'toppanel@hydrator.detail': {
              templateUrl: '/assets/features/hydrator/templates/detail/top-panel.html',
              controller: 'HydratorDetailTopPanelController',
              controllerAs: 'TopPanelCtrl'
            },
            'canvas@hydrator.detail': {
              templateUrl: '/assets/features/hydrator/templates/detail/canvas.html',
              controller: 'HydratorPlusPlusDetailCanvasCtrl',
              controllerAs: 'CanvasCtrl'
            }
          },
          onExit: function($uibModalStack) {
            $uibModalStack.dismissAll();
          }
        })

        .state('hydrator.list', {
          url: '?page',
          title: 'Published Pipelines',
          onEnter: function() {
            document.title = 'Published Pipelines';
          },
          data: {
            authorizedRoles: MYAUTH_ROLE.all,
            highlightTab: 'hydratorList'
          },
          templateUrl: '/assets/features/hydrator/templates/list.html',
          controller: 'HydratorPlusPlusListController',
          controllerAs: 'ListController'
        });
  });
