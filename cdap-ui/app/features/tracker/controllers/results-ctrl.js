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

class TrackerResultsController {
  constructor($state, myTrackerApi, $scope, $q) {
    this.$state = $state;
    this.$scope = $scope;
    this.myTrackerApi = myTrackerApi;
    this.$q = $q;

    this.loading = false;
    this.searchResults = [];

    this.fetchResults();
  }

  /* Can be updated to use single query once backend supports it */
  fetchResults () {
    this.loading = true;

    let paramsBase = {
      namespace: this.$state.params.namespace,
      query: this.$state.params.searchQuery,
      scope: this.$scope
    };
    let datasetParams = angular.extend({target: 'dataset'}, paramsBase);
    let streamParams = angular.extend({target: 'stream'}, paramsBase);
    let viewParams = angular.extend({target: 'view'}, paramsBase);

    this.$q.all([
      this.myTrackerApi.search(datasetParams).$promise,
      this.myTrackerApi.search(streamParams).$promise,
      this.myTrackerApi.search(viewParams).$promise
    ]).then( (res) => {
      this.searchResults = this.searchResults.concat(res[0], res[1], res[2]);
      this.loading = false;
    }, (err) => {
      console.log('error', err);
      this.loading = false;
    });

  }
}

TrackerResultsController.$inject = ['$state', 'myTrackerApi', '$scope', '$q'];

angular.module(PKG.name + '.feature.tracker')
 .controller('TrackerResultsController', TrackerResultsController);
