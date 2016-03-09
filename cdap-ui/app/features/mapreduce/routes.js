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

angular.module(PKG.name + '.feature.mapreduce')
  .config(function($stateProvider, $urlRouterProvider, MYAUTH_ROLE) {
    $stateProvider
      .state('mapreduce', {
        url: '/mapreduce/:programId',
        abstract: true,
        parent: 'programs',
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'development'
        },
        resolve: {
          rRuns: function($stateParams, $q, myMapreduceApi, $state) {
            var defer = $q.defer();

            var params = {
              namespace: $stateParams.namespace,
              appId: $stateParams.appId,
              mapreduceId: $stateParams.programId
            };
            myMapreduceApi.runs(params)
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
          rMapreduceDetail: function($stateParams, myMapreduceApi, $q, $state) {
            var params = {
              namespace: $stateParams.namespace,
              appId: $stateParams.appId,
              mapreduceId: $stateParams.programId
            };
            var defer = $q.defer();
            myMapreduceApi
              .get(params)
              .$promise
              .then(
                function success(mapreduceDetail) {
                  defer.resolve(mapreduceDetail);
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

      .state('mapreduce.detail', {
        url: '/runs',
        templateUrl: '/assets/features/mapreduce/templates/detail.html',
        controller: 'MapreduceRunsController',
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
        .state('mapreduce.detail.run', {
          url: '/:runid?sourceId&sourceRunId&destinationType',
          templateUrl: '/assets/features/mapreduce/templates/tabs/runs/run-detail.html',
          controller: 'MapreduceRunsDetailController',
          controllerAs: 'RunsDetailController',
          ncyBreadcrumb: {
            label: '{{$state.params.runid}}',
            parent: 'mapreduce.detail'
          }
        });

  });
