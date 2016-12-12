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

angular.module(`${PKG.name}.feature.workflows`)
  .config(function($stateProvider, $urlRouterProvider, MYAUTH_ROLE) {
    $stateProvider
      .state('workflows', {
        url: '/workflows/:programId',
        abstract: true,
        parent: 'programs',
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'development'
        },
        resolve : {
          rRuns: function(MyCDAPDataSource, $stateParams, $q, $state) {
            var defer = $q.defer();
            var dataSrc = new MyCDAPDataSource();
            // Using _cdapPath here as $state.params is not updated with
            // runid param when the request goes out
            // (timing issue with re-direct from login state).
            dataSrc.request({
              _cdapPath: `/namespaces/${$stateParams.namespace}/apps/${$stateParams.appId}/workflows/${$stateParams.programId}/runs`
            })
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
          rWorkflowDetail: function(myWorkFlowApi, $stateParams, $q, $state) {
            var params = {
              namespace: $stateParams.namespace,
              appId: $stateParams.appId,
              workflowId: $stateParams.programId
            };
            var defer = $q.defer();
            myWorkFlowApi
              .get(params)
              .$promise
              .then(
                function success(workflowDetail) {
                  defer.resolve(workflowDetail);
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

        .state('workflows.detail', {
          url: '/runs',
          templateUrl: '/old_assets/features/workflows/templates/detail.html',
          controller: 'WorkflowsRunsController',
          controllerAs: 'RunsController',
          data: {
            authorizedRoles: MYAUTH_ROLE.all,
            highlightTab: 'development'
          },
          ncyBreadcrumb: {
            parent: 'apps.detail.overview.programs',
            label: '{{$state.params.programId}}'
          },
          onExit: function($uibModalStack) {
            $uibModalStack.dismissAll();
          }
        })

          .state('workflows.detail.run', {
            url: '/:runid',
            templateUrl: '/old_assets/features/workflows/templates/tabs/runs/run-detail.html',
            controller: 'WorkflowsRunsDetailController',
            controllerAs: 'RunsDetailController',
            ncyBreadcrumb: {
              label: '{{$state.params.runid}}',
              parent: 'workflows.detail'
            },
            onExit: function($uibModalStack) {
              $uibModalStack.dismissAll();
            }
          });
  });
