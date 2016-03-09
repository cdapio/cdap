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

angular.module(PKG.name + '.feature.spark')
  .config(function($stateProvider, $urlRouterProvider, MYAUTH_ROLE) {
    $stateProvider
      .state('spark', {
        url: '/spark/:programId',
        abstract: true,
        parent: 'programs',
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'development'
        },
        resolve : {
          rRuns: function($stateParams, $q, mySparkApi, $state) {
            var defer = $q.defer();

            var params = {
              namespace: $stateParams.namespace,
              appId: $stateParams.appId,
              sparkId: $stateParams.programId
            };
            mySparkApi.runs(params)
              .$promise
              .then(
                function success(res) {
                  defer.resolve(res);
                },
                function error() {
                  defer.reject();
                  $state.go('404');
                }
              );

            return defer.promise;
          },
          rSparkDetail: function($stateParams, mySparkApi, $q, $state) {
            var params = {
              namespace: $stateParams.namespace,
              appId: $stateParams.appId,
              sparkId: $stateParams.programId
            };
            var defer = $q.defer();
            mySparkApi
              .get(params)
              .$promise
              .then(
                function success(sparkDetail) {
                  defer.resolve(sparkDetail);
                },
                function error() {
                  defer.reject();
                  $state.go('404');
                }
              );
            return defer.promise;
          }
        },
        template: '<ui-view/>'
      })

      .state('spark.detail', {
        url: '/runs',
        templateUrl: '/assets/features/spark/templates/detail.html',
        controller: 'SparkRunsController',
        controllerAs: 'RunsController',
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'development'
        },
        ncyBreadcrumb: {
          parent: 'apps.detail.overview.programs',
          label: '{{$state.params.programId}}'
        }
      })

      .state('spark.detail.run', {
        url: '/:runid?sourceId&sourceRunId&destinationType',
        templateUrl: '/assets/features/spark/templates/tabs/runs/run-detail.html',
        controller: 'SparkRunDetailController',
        controllerAs: 'RunsDetailController',
        ncyBreadcrumb: {
          label: '{{$state.params.runid}}',
          parent: 'spark.detail'
        }
      });

  });
