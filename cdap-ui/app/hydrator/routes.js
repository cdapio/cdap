/*
 * Copyright © 2015-2018 Cask Data, Inc.
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
  .config(function($stateProvider, $urlRouterProvider, MYAUTH_ROLE, GLOBALS) {
    const theme = window.CaskCommon.ThemeHelper.Theme;
    const productName = theme.productName;
    const featureName = theme.featureNames.pipelines;

    const uiSupportedArtifacts = [GLOBALS.etlDataPipeline];

    if (theme.showRealtimePipeline !== false) {
      uiSupportedArtifacts.push(GLOBALS.etlDataStreams);
    }

    if (theme.showSqlPipeline !== false) {
      uiSupportedArtifacts.push(GLOBALS.eltSqlPipeline);
    }

    $urlRouterProvider.otherwise(() => {
      //Unmatched route, will show 404
      window.CaskCommon.ee.emit(
        window.CaskCommon.globalEvents.PAGE_LEVEL_ERROR, { statusCode: 404 });

    });

    $stateProvider
      .state('home', {
        url: '/',
        template: '<ui-view/>',
        resolve: {
          sessionToken: function() {
            window.CaskCommon.SessionTokenStore.fetchSessionToken();
          },
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
          sessionToken: function() {
            window.CaskCommon.SessionTokenStore.fetchSessionToken();
          },
          // This is f%&$*d up. We need to cause this manual delay for react to finish its click handlers
          // before angular takes up the state change routing -_-.
          rDelay: function($q) {
            var defer = $q.defer();
            setTimeout(() => {
              defer.resolve();
            });
            return defer.promise;
          },
          rResetPreviousPageLevelError: function () {
            window.CaskCommon.ee.emit(
              window.CaskCommon.globalEvents.PAGE_LEVEL_ERROR, { reset: true });
          },
          rValidNamespace: function($stateParams){
            return window.CaskCommon.validateNamespace($stateParams.namespace);
          },
        },
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'development'
        }
      })
        .state('hydrator.create', {
          url: '/studio?artifactType&draftId&workspaceId&configParams&rulesengineid&resourceCenterId&cloneId',
          onEnter: function() {
            document.title = `${productName} | Studio`;
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
             rCDAPVersion: function($q) {
              var defer = $q.defer();
              let cdapversion = window.CaskCommon.VersionStore.getState().version;
              if (cdapversion) {
                defer.resolve(cdapversion);
                return defer.promise;
              }
              const subscription = window.CaskCommon.VersionStore.subscribe(() => {
                let cdapversion = window.CaskCommon.VersionStore.getState().version;
                if (cdapversion) {
                  defer.resolve(cdapversion);
                  subscription();
                }
              });
              return defer.promise;
            },
            rResetPreviousPageLevelError: function () {
              window.CaskCommon.ee.emit(
                window.CaskCommon.globalEvents.PAGE_LEVEL_ERROR, { reset: true });
            },
            rConfig: function(rCDAPVersion, $stateParams, mySettings, $q, myHelpers, $window, HydratorPlusPlusHydratorService, myPipelineApi) {
              var defer = $q.defer();
              const processDraft = (draft) => {
                if (angular.isObject(draft)) {
                  let isVersionInRange = HydratorPlusPlusHydratorService
                    .isVersionInRange({
                      supportedVersion: rCDAPVersion,
                      versionRange: draft.artifact.version
                    });

                  if (isVersionInRange) {
                    draft.artifact.version = rCDAPVersion;
                  } else {
                    defer.resolve({ valid: false, config: draft, upgrade: true });
                    return;
                  }
                  defer.resolve({ valid: true, config: draft });
                } else {
                  defer.resolve({ valid: false });
                }
              };
              if ($stateParams.data) {
                // This is being used while cloning a published a pipeline.
                let isVersionInRange = HydratorPlusPlusHydratorService
                  .isVersionInRange({
                    supportedVersion: rCDAPVersion,
                    versionRange: $stateParams.data.artifact.version
                  });
                if (isVersionInRange) {
                  $stateParams.data.artifact.version = rCDAPVersion;
                } else {
                  defer.resolve({ valid: false });
                }
                defer.resolve({
                  config: $stateParams.data,
                  valid: true
                });
                return defer.promise;
              }
              if ($stateParams.draftId) {
                const params = {
                  context: $stateParams.namespace,
                  draftId: $stateParams.draftId,
                };
                myPipelineApi.getDraft(params)
                  .$promise
                  .then(processDraft, () => {
                    mySettings.get('hydratorDrafts', true)
                      .then(function(res) {
                        processDraft(myHelpers.objectQuery(res, $stateParams.namespace, $stateParams.draftId));
                      });
                  });
              } else if ($stateParams.configParams) {
                // This is being used while adding a dataset/stream as source/sink from metadata to pipeline studio
                try {
                  let config = JSON.parse($stateParams.configParams);
                  defer.resolve({ valid: true, config });
                } catch (e) {
                  defer.resolve({ valid: false });
                }
              } else if ($stateParams.workspaceId) {
                // This is being used by dataprep to pipelines transition
                try {
                  let configParams = $window.localStorage.getItem($stateParams.workspaceId);
                  let config = JSON.parse(configParams);
                  defer.resolve({ valid: true, config });
                } catch (e) {
                  defer.resolve({ valid: false });
                }
                $window.localStorage.removeItem($stateParams.workspaceId);
              } else if ($stateParams.rulesengineid) {
                try {
                  let configParams = $window.localStorage.getItem($stateParams.rulesengineid);
                  let config = JSON.parse(configParams);
                  defer.resolve({ valid: true, config });
                } catch (e) {
                  defer.resolve({ valid: false });
                }
                $window.localStorage.removeItem($stateParams.rulesengineid);
              } else if ($stateParams.cloneId) {
                try {
                  let configParams = $window.localStorage.getItem($stateParams.cloneId);
                  let config = JSON.parse(configParams);
                  defer.resolve({ valid: true, config });
                } catch (e) {
                  defer.resolve({ valid: false });
                }
                $window.localStorage.removeItem($stateParams.cloneId);
              } else {
                defer.resolve({ valid: false });
              }
              return defer.promise;
            },
            rSelectedArtifact: function(rCDAPVersion, $stateParams, $q, myPipelineApi) {
              var defer = $q.defer();
              let isArtifactValid = (backendArtifacts, artifact) => {
                return backendArtifacts.filter( a =>
                  (a.name === artifact && a.version === rCDAPVersion)
                ).length;
              };
              let isAnyUISupportedArtifactPresent = (backendArtifacts) => {
                return backendArtifacts
                        .filter( artifact => artifact.version === rCDAPVersion)
                        .filter( artifact => uiSupportedArtifacts.indexOf(artifact.name) !== -1);
              };
              let getValidUISupportedArtifact = (backendArtifacts) => {
                let validUISupportedArtifact = isAnyUISupportedArtifactPresent(backendArtifacts);
                return validUISupportedArtifact.length ?  validUISupportedArtifact[0]: false;
              };

              let showError = (error) => {
                window.CaskCommon.ee.emit(window.CaskCommon.globalEvents.PAGE_LEVEL_ERROR, error);
              };

              myPipelineApi.fetchArtifacts({
                namespace: $stateParams.namespace
              }).$promise.then((artifactsFromBackend) => {
                let showNoArtifactsError = () => {
                  showError({ data: GLOBALS.en.hydrator.studio.error['MISSING-SYSTEM-ARTIFACTS'], statusCode: 404 });
                };

                let chooseDefaultArtifact = () => {
                  if (!isArtifactValid(artifactsFromBackend, GLOBALS.etlDataPipeline)) {
                    if (!isAnyUISupportedArtifactPresent(artifactsFromBackend).length) {
                      return showNoArtifactsError();
                    } else {
                      $stateParams.artifactType = getValidUISupportedArtifact(artifactsFromBackend).name;
                      defer.resolve($stateParams.artifactType);
                    }
                  } else {
                    $stateParams.artifactType = GLOBALS.etlDataPipeline;
                    defer.resolve($stateParams.artifactType);
                  }
                };

                if (!artifactsFromBackend.length) {
                  return showNoArtifactsError();
                }

                if (!isArtifactValid(artifactsFromBackend, $stateParams.artifactType)) {
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
            rArtifacts: function(rCDAPVersion, myPipelineApi, $stateParams, $q, HydratorPlusPlusOrderingFactory) {
              var defer = $q.defer();
              myPipelineApi.fetchArtifacts({
                namespace: $stateParams.namespace
              }).$promise
              .then((res) => {
                if (!res.length) {
                  return;
                } else {
                  let filteredRes = res
                    .filter( artifact => artifact.version === rCDAPVersion)
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
          url: '/view/:pipelineId?runid',
          data: {
            authorizedRoles: MYAUTH_ROLE.all,
            highlightTab: 'hydratorList'
          },
          onEnter: function($stateParams) {
            document.title = `${productName} | ${featureName} | ${$stateParams.pipelineId}`;
          },
          resolve : {
            rPipelineDetail: function($stateParams, $q, myPipelineApi, myAlertOnValium, $window) {
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
                    } catch (e) {
                      myAlertOnValium.show({
                        type: 'danger',
                        content: 'Invalid configuration JSON.'
                      });
                      $q.reject(false);
                      // FIXME: We should not have done this. But ui-router when rejected on a 'resolve:' function takes it to the parent state apparently
                      // and in our case the parent state is 'hydrator and since its an abstract state it goes to home.'
                      $window.location.href = $window.getHydratorUrl({
                        stateName: 'hydrator.list',
                        stateParams: {
                          namespace: $stateParams.namespace
                        },
                      });
                      return;
                    }
                    if (!config.stages) {
                      myAlertOnValium.show({
                        type: 'danger',
                        content: 'Pipeline is created using older version of hydrator. Please upgrage the pipeline to newer version(3.4) to view in UI.'
                      });
                      $q.reject(false);
                      // FIXME: We should not have done this. But ui-router when rejected on a 'resolve:' function takes it to the parent state apparently
                      // and in our case the parent state is 'hydrator and since its an abstract state it goes to home.'
                      $window.location.href = $window.getHydratorUrl({
                        stateName: 'hydrator.list',
                        stateParams: {
                          namespace: $stateParams.namespace
                        },
                      });
                      return;
                    }
                    return $q.resolve(pipelineDetail);
                  },(err) => {
                      window.CaskCommon.ee.emit(
                        window.CaskCommon.globalEvents.PAGE_LEVEL_ERROR, err);
                    }
                );
            },
            rResetPreviousPageLevelError: function () {
              window.CaskCommon.ee.emit(
                window.CaskCommon.globalEvents.PAGE_LEVEL_ERROR, { reset: true });
            },
          },
          ncyBreadcrumb: {
            parent: 'apps.list',
            label: '{{$state.params.pipelineId}}'
          },
          views: {
            '': {
              templateUrl: '/assets/features/hydrator/templates/detail.html',
              controller: 'HydratorPlusPlusDetailCtrl',
              controllerAs: 'DetailCtrl'
            },
            'toppanel@hydrator.detail': {
              templateUrl: '/assets/features/hydrator/templates/detail/top-panel.html'
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
        });
  });
