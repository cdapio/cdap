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

angular.module(PKG.name+'.feature.experimental')
  .config(function ($stateProvider) {
    $stateProvider
      .state('experimental', {
        parent: 'ns',
        url: '/experimental',
        abstract: true,
        templateUrl: '/assets/features/experimental/templates/experimental.html',
      })
        .state('experimental.home', {
          url: '',
          templateUrl: '/assets/features/experimental/templates/experiments-home.html',
          ncyBreadcrumb: {
            label: 'Experiments',
            parent: 'experimental'
          }
        })
        .state('experimental.uitemplate', {
          url: '/uitemplate',
          templateUrl: '/assets/features/experimental/templates/ui-template.html',
          ncyBreadcrumb: {
            label: 'UI Templates',
            parent: 'experimental.home'
          }
        })
        .state('experimental.directivesPlayground', {
          url: '/experimental-directives',
          templateUrl: '/assets/features/experimental/templates/directives-playground.html',
          ncyBreadcrumb: {
            label: 'Directives Playground',
            parent: 'experimental.home'
          }
        })
        .state('experimental.datetime', {
          url: '/datetime',
          templateUrl: '/assets/features/experimental/templates/datetime.html',
          controller: 'DateCtrl',
          controllerAs: 'Date',
          ncyBreadcrumb: {
            label: 'Datetime Playground',
            parent: 'experimental.home'
          }
        })
        .state('experimental.schema', {
          url: '/schema',
          templateUrl: '/assets/features/experimental/templates/schema.html',
          controller: 'SchemaController',
          controllerAs: 'vm',
          ncyBreadcrumb: {
            label: 'Schema Widget',
            parent: 'experimental.home'
          }
        });

  });
