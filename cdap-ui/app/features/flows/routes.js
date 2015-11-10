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

angular.module(`${PKG.name}.feature.flows`)
  .config(function($stateProvider, $urlRouterProvider, MYAUTH_ROLE) {
    $stateProvider
      .state('flows', {
        url: '/flows/:programId',
        abstract: true,
        parent: 'programs',
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'development'
        },
        resolve : {
          rRuns: function($stateParams, $q, myFlowsApi, $state) {
            var defer = $q.defer();

            // Using _cdapPath here as $state.params is not updated with
            // runid param when the request goes out
            // (timing issue with re-direct from login state).
            var params = {
              namespace: $stateParams.namespace,
              appId: $stateParams.appId,
              flowId: $stateParams.programId
            };
            myFlowsApi.runs(params)
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
          rFlowsDetail: function($stateParams, myFlowsApi, $q, $state) {
            var params = {
              namespace: $stateParams.namespace,
              appId: $stateParams.appId,
              flowId: $stateParams.programId
            };
            var defer = $q.defer();
            myFlowsApi
              .get(params)
              .$promise
              .then(
                function success(flowsDetail) {
                  defer.resolve(flowsDetail);
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

      .state('flows.detail', {
        url: '/runs',
        templateUrl: '/assets/features/flows/templates/detail.html',
        controller: 'FlowsRunsController',
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

      .state('flows.detail.run', {
        url: '/:runid',
        templateUrl: '/assets/features/flows/templates/tabs/runs/run-detail.html',
        controller: 'FlowsRunDetailController',
        ncyBreadcrumb: {
          label: '{{$state.params.runid}}',
          parent: 'flows.detail'
        }
      });
  });
