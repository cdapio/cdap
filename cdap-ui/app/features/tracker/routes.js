/*
 * Copyright © 2016 Cask Data, Inc.
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

angular.module(PKG.name + '.feature.tracker')
  .config(function($stateProvider, MYAUTH_ROLE) {

    $stateProvider
      .state('tracker-enable', {
        url: '/enable-tracker',
        parent: 'ns',
        templateUrl: '/assets/features/tracker/templates/tracker-enable.html',
        controller: 'TrackerEnableController',
        controllerAs: 'EnableController',
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
        },
        resolve: {
          rTrackerApp: function (myTrackerApi, $q, $stateParams, $state, UI_CONFIG, myAlertOnValium, $cookies) {
            let defer = $q.defer();
            let params = {
              namespace: $stateParams.namespace || $cookies.get('CDAP_Namespace') || 'default'
            };

            myTrackerApi.getTrackerApp(params)
              .$promise
              .then( () => {
                let trackerServiceParams = {
                  namespace: $stateParams.namespace,
                  programType: 'services',
                  programId: UI_CONFIG.tracker.serviceId
                };

                let auditFlowParams = {
                  namespace: $stateParams.namespace,
                  programType: 'flows',
                  programId: UI_CONFIG.tracker.flowProgramId
                };

                $q.all([
                  myTrackerApi.trackerProgramStatus(trackerServiceParams).$promise,
                  myTrackerApi.trackerProgramStatus(auditFlowParams).$promise,
                ]).then( (res) => {
                  let isRunning = true;
                  angular.forEach(res, (program) => {
                    if (program.status === 'STOPPED') {
                      isRunning = false;
                    }
                  });

                  if (isRunning) {
                    $state.go('tracker.home', $stateParams);
                  } else {
                    defer.resolve(true);
                  }
                }, (err) => {
                  myAlertOnValium.show({
                    type: 'danger',
                    content: err.data
                  });
                });
              }, () => {
                defer.resolve(true);
              });

            return defer.promise;
          }
        }
      })
      .state('tracker', {
        url: '/tracker',
        abstract: true,
        parent: 'ns',
        template: '<ui-view/>',
        resolve: {
          rTrackerApp: function (myTrackerApi, $q, $stateParams, $state, UI_CONFIG, myAlertOnValium, $cookies) {
            let defer = $q.defer();
            let params = {
              namespace: $stateParams.namespace || $cookies.get('CDAP_Namespace') || 'default'
            };

            myTrackerApi.getTrackerApp(params)
              .$promise
              .then( () => {
                let trackerServiceParams = {
                  namespace: $stateParams.namespace,
                  programType: 'services',
                  programId: UI_CONFIG.tracker.serviceId
                };

                let auditFlowParams = {
                  namespace: $stateParams.namespace,
                  programType: 'flows',
                  programId: UI_CONFIG.tracker.flowProgramId
                };

                $q.all([
                  myTrackerApi.trackerProgramStatus(trackerServiceParams).$promise,
                  myTrackerApi.trackerProgramStatus(auditFlowParams).$promise,
                ]).then( (res) => {
                  let isRunning = true;
                  angular.forEach(res, (program) => {
                    if (program.status === 'STOPPED') {
                      isRunning = false;
                    }
                  });

                  if (!isRunning) {
                    $state.go('tracker-enable', $stateParams);
                  } else {
                    defer.resolve(true);
                  }
                }, (err) => {
                  myAlertOnValium.show({
                    type: 'danger',
                    content: err.data
                  });
                });
              }, () => {
                $state.go('tracker-enable', $stateParams);
              });

            return defer.promise;
          }
        }
      })

      .state('tracker.home', {
        url: '/home',
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'search'
        },
        templateUrl: '/assets/features/tracker/templates/main.html',
        controller: 'TrackerMainController',
        controllerAs: 'MainController'
      })

      .state('tracker.integrations', {
        url: '/integrations',
        templateUrl: '/assets/features/tracker/templates/integrations.html',
        controller: 'TrackerIntegrationsController',
        controllerAs: 'IntegrationsController',
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'integrations'
        }
      })

      .state('tracker.tags', {
        url: '/tags',
        templateUrl: '/assets/features/tracker/templates/tags.html',
        controller: 'TrackerTagsController',
        controllerAs: 'TagsController',
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'tags'
        }
      })

      .state('tracker.detail', {
        url: '',
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'search'
        },
        templateUrl: '/assets/features/tracker/templates/container.html',
        controller: 'TrackerContainerController',
        controllerAs: 'ContainerController'
      })

        .state('tracker.detail.result', {
          url: '/search/:searchQuery/result',
          templateUrl: '/assets/features/tracker/templates/results.html',
          controller: 'TrackerResultsController',
          controllerAs: 'ResultsController',
          data: {
            authorizedRoles: MYAUTH_ROLE.all,
            highlightTab: 'search'
          }
        })

        .state('tracker.detail.entity', {
          url: '/entity/:entityType/:entityId?searchTerm',
          templateUrl: '/assets/features/tracker/templates/entity.html',
          controller: 'TrackerEntityController',
          controllerAs: 'EntityController',
          data: {
            authorizedRoles: MYAUTH_ROLE.all,
            highlightTab: 'search'
          },
          resolve: {
            rDatasetType: function ($q, myTrackerApi, $stateParams) {
              if ($stateParams.entityType !== 'datasets') {
                return null;
              }

              let defer = $q.defer();

              let params = {
                namespace: $stateParams.namespace,
                entityType: $stateParams.entityType,
                entityId: $stateParams.entityId
              };
              myTrackerApi.getDatasetSystemProperties(params)
                .$promise
                .then( (res) => {
                  defer.resolve(res.type);
                }, () => {
                  defer.reject();
                });

              return defer.promise;
            },
            rSystemTags: function ($q, myTrackerApi, $stateParams) {
              if ($stateParams.entityType !== 'datasets') {
                return null;
              }

              let defer = $q.defer();

              let params = {
                namespace: $stateParams.namespace,
                entityType: $stateParams.entityType,
                entityId: $stateParams.entityId
              };
              myTrackerApi.getSystemTags(params)
                .$promise
                .then( defer.resolve, defer.reject);

              return defer.promise;
            }
          }
        })
          .state('tracker.detail.entity.metadata', {
            url: '/metadata',
            templateUrl: '/assets/features/tracker/templates/metadata.html',
            controller: 'TrackerMetadataController',
            controllerAs: 'MetadataController',
            data: {
              authorizedRoles: MYAUTH_ROLE.all,
              highlightTab: 'search'
            }
          })
          .state('tracker.detail.entity.lineage', {
            url: '/lineage?start&end&method',
            templateUrl: '/assets/features/tracker/templates/lineage.html',
            controller: 'TrackerLineageController',
            controllerAs: 'LineageController',
            data: {
              authorizedRoles: MYAUTH_ROLE.all,
              highlightTab: 'search'
            }
          })
          .state('tracker.detail.entity.audit', {
            url: '/audit?start&end',
            templateUrl: '/assets/features/tracker/templates/audit.html',
            controller: 'TrackerAuditController',
            controllerAs: 'AuditController',
            data: {
              authorizedRoles: MYAUTH_ROLE.all,
              highlightTab: 'search'
            }
          })
          .state('tracker.detail.entity.usage', {
            url: '/usage?start&end',
            templateUrl: '/assets/features/tracker/templates/usage.html',
            controller: 'TrackerUsageController',
            controllerAs: 'UsageController',
            data: {
              authorizedRoles: MYAUTH_ROLE.all,
              highlightTab: 'search'
            }
          })
          .state('tracker.detail.entity.preview', {
            url: '/preview',
            templateUrl: '/assets/features/tracker/templates/preview.html',
            controller: 'TrackerPreviewController',
            controllerAs: 'PreviewController',
            data: {
              authorizedRoles: MYAUTH_ROLE.all,
              highlightTab: 'search'
            }
          });
  });
