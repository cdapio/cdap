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

angular.module(PKG.name + '.feature.streams')
  .config(function($stateProvider, $urlRouterProvider, MYAUTH_ROLE) {
    $stateProvider
      .state('streams', {
        abstract: true,
        template: '<ui-view/>',
        url: '/streams',
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'development'
        },
        parent: 'ns'
      })

      .state('streams.detail', {
        url: '/:streamId',
        abstract: true,
        template: '<ui-view/>'
      })
        .state('streams.detail.overview', {
          url: '/overview',
          parent: 'streams.detail',
          resolve: {
            rStreamData: function($state, $q, myStreamApi, $stateParams) {
              var params = {
                namespace: $stateParams.namespace,
                streamId: $stateParams.streamId
              };
              var defer = $q.defer();

              myStreamApi
                .get(params)
                .$promise
                .then(
                  function success(streamDetail) {
                    defer.resolve(streamDetail);
                  },
                  function error() {
                    defer.reject();
                    $state.go('404');
                  }
                );
              return defer.promise;
            }
          },
          templateUrl: '/assets/features/streams/templates/detail.html',
          controller: 'CdapStreamDetailController',
          controllerAs: 'DetailController',
          ncyBreadcrumb: {
            parent: 'data.list',
            label: '{{$state.params.streamId}}'
          }
        })

        .state('streams.detail.overview.status', {
          url: '/status',
          templateUrl: '/assets/features/streams/templates/tabs/status.html',
          ncyBreadcrumb: {
            parent: 'data.list',
            label: '{{$state.params.streamId}}'
          },
          controller: 'StreamDetailStatusController',
          controllerAs: 'StatusController'
        })

        .state('streams.detail.overview.explore', {
          url: '/explore',
          templateUrl: '/assets/features/streams/templates/tabs/explore.html',
          controller: 'StreamExploreController',
          controllerAs: 'ExploreController',
          ncyBreadcrumb: {
            label: 'Explore',
            parent: 'streams.detail.overview.status'
          }
        })

        .state('streams.detail.overview.programs', {
          url: '/programs',
          templateUrl: '/assets/features/streams/templates/tabs/programs.html',
          controller: 'StreamProgramsController',
          controllerAs: 'ProgramsController',
          ncyBreadcrumb: {
            label: 'Programs',
            parent: 'streams.detail.overview.status'
          }
        })

        .state('streams.detail.overview.metadata', {
          url: '/metadata',
          templateUrl: '/assets/features/streams/templates/tabs/metadata.html',
          controller: 'StreamMetadataController',
          controllerAs: 'MetadataController',
          ncyBreadcrumb: {
            label: 'Metadata',
            parent: 'streams.detail.overview.status'
          }
        });
});
