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

angular.module(`${PKG.name}.feature.apps`)
  .config(function ($stateProvider, $urlRouterProvider, MYAUTH_ROLE) {

    /**
     * State Configurations
     */
    $stateProvider

      .state('apps', {
        abstract: true,
        template: '<ui-view/>',
        url: '/apps',
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'development'
        },
        parent: 'ns'
      })

      .state('apps.list', {
        url: '',
        templateUrl: '/assets/features/apps/templates/list.html',
        controller: 'AppListController',
        controllerAs: 'ListController',
        ncyBreadcrumb: {
          label: 'Applications',
          parent: 'overview'
        }
      })

      .state('apps.detail', {
        url: '/:appId',
        abstract: true,
        template: '<ui-view/>'
      })
        .state('apps.detail.overview', {
          url: '/overview',
          templateUrl: '/assets/features/apps/templates/detail.html',
          resolve: {
            rAppData: function(MyCDAPDataSource, $stateParams, $q, $state) {
              var datasrc = new MyCDAPDataSource();
              var defer = $q.defer();
              datasrc.request({
                _cdapPath: '/namespaces/' + $stateParams.namespace + '/apps/' + $stateParams.appId
              })
                .then(
                  function success(appDetail) {
                    defer.resolve(appDetail);
                  },
                  function error() {
                    defer.reject();
                    $state.go('404');
                  }
                );
              return defer.promise;
            }
          },
          controller: 'AppDetailController',
          controllerAs: 'DetailController',
          ncyBreadcrumb: {
            skip: true
          }
        })
          .state('apps.detail.overview.status', {
            url: '/status',
            ncyBreadcrumb: {
              parent: 'apps.list',
              label: '{{$state.params.appId}}'
            },
            controller: 'AppDetailStatusController',
            controllerAs: 'StatusController',
            templateUrl: '/assets/features/apps/templates/tabs/status.html',
            resolve : {
              rPipelineDetail: function($stateParams, $q, myPipelineApi) {
                var params = {
                  namespace: $stateParams.namespace,
                  pipeline: $stateParams.appId
                };

                return myPipelineApi.get(params).$promise;
              }
            }
          })

          .state('apps.detail.overview.programs', {
            url: '/programs',
            ncyBreadcrumb: {
              parent: 'apps.list',
              label: '{{$state.params.appId}}'
            },
            controller: 'AppDetailProgramsController',
            controllerAs: 'ProgramsController',
            templateUrl: '/assets/features/apps/templates/tabs/programs.html'
          })

          .state('apps.detail.overview.datasets', {
            url: '/datasets',
            ncyBreadcrumb: {
              label: 'Datasets',
              parent: 'apps.detail.overview.status'
            },
            templateUrl: '/assets/features/apps/templates/tabs/datasets.html'
          });

  });
