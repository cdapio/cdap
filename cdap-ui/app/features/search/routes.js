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

angular.module(`${PKG.name}.feature.search`)
  .config(function($stateProvider) {
    $stateProvider
      .state('search', {
        parent: 'ns',
        url: '/search',
        abstract: true,
        templateUrl: '/old_assets/features/search/templates/search.html'
      })
        .state('search.objectswithtags', {
          url: '/:tag',
          templateUrl: '/old_assets/features/search/templates/search-objects-with-tags.html',
          controller: 'SearchObjectWithTagsController',
          controllerAs: 'SearchObjectWithTagsController',
          ncyBreadcrumb: {
            label: 'Search Results'
          }
        });
  });
