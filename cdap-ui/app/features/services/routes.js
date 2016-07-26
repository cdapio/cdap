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

angular.module(PKG.name + '.feature.services')
  .config(function($stateProvider, $urlRouterProvider, MYAUTH_ROLE) {
    $stateProvider
      .state('services', {
        url: '/services/:programId',
        abstract: true,
        parent: 'programs',
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'development'
        },
        resolve : {
          rRuns: function($stateParams, $q, myServiceApi, $state) {
            var defer = $q.defer();

            var params = {
              namespace: $stateParams.namespace,
              appId: $stateParams.appId,
              serviceId: $stateParams.programId
            };
            myServiceApi.runs(params)
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
          rServiceDetail: function($stateParams, myServiceApi, $q, $state) {
            var params = {
              namespace: $stateParams.namespace,
              appId: $stateParams.appId,
              serviceId: $stateParams.programId
            };
            var defer = $q.defer();
            myServiceApi
              .get(params)
              .$promise
              .then(
                function success(serviceDetail) {
                  defer.resolve(serviceDetail);
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
      .state('services.detail', {
        url: '/runs',
        templateUrl: '/assets/features/services/templates/detail.html',
        controller: 'ServicesRunsController',
        controllerAs: 'RunsController',
        ncyBreadcrumb: {
          parent: 'apps.detail.overview.programs',
          label: '{{$state.params.programId}}'
        }
      })
        .state('services.detail.run', {
          url: '/:runid',
          templateUrl: '/assets/features/services/templates/tabs/runs/run-detail.html',
          controller: 'ServicesRunsDetailController',
          controllerAs: 'RunsDetailController',
          ncyBreadcrumb: {
            label: '{{$state.params.runid}}',
            parent: 'services.detail'
          }
        });
  });
