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

class SearchController {
  constructor(myTagsApi, myAppsApi, myDatasetApi, myStreamApi, $stateParams, $state, caskFocusManager, $scope) {
    this.myTagsApi = myTagsApi;
    this.myAppsApi = myAppsApi;
    this.myDatasetApi = myDatasetApi;
    this.myStreamApi = myStreamApi;
    this.$stateParams = $stateParams;
    this.loadingData = true;
    this.$state = $state;
    if ($stateParams.searchTag) {
      this.searchTxt = $stateParams.searchTag;
    }

    $scope.$watch(
      () => this.searchTxt ,
      () => {
        $stateParams.searchTag = this.searchTxt;
        $state.transitionTo($state.current, $stateParams, {notify: false});
      }
    );

    this.tags = [];
    caskFocusManager.select('searchByTags');

    this.getAppsTags();
    this.getDatasetsTags();
    this.getStreamsTags();
  }

  getAppsTags() {
    let params = {
      namespace: this.$stateParams.namespace
    };
    this.myAppsApi
      .list(params)
      .$promise
      .then(
        (apps) => {
          apps.forEach((app) => {
            this.fetchAppTags(app.id);
            this.getProgramTags(app.id);
          });
        },
        () => { console.error('Apps Fetch errored'); }
      );
  }
  fetchAppTags(appId) {
    let params = {
      namespaceId: this.$stateParams.namespace,
      appId: appId
    };
    this.myTagsApi
      .getAppTags(params)
      .$promise
      .then(
        (appTags) => {
          this.checkAndMarkLoading(appTags.length);
          this.tags = this.tags.concat(appTags);
        },
        () => { console.error('Error on fetching tags for app: ', appId); }
      );
  }
  getProgramTags(appId) {
    let params = {
      namespace: this.$stateParams.namespace,
      appId: appId
    };
    this.myAppsApi
      .get(params)
      .$promise
      .then(
        (app) => {
          app.programs.forEach( (program) => {
            this.fetchProgramTags(appId, program.id, program.type.toLowerCase());
          });
        },
        () => {
          console.error('Error fetching tags for programs');
        }
      );
  }
  getDatasetsTags() {
    let params = {
      namespace: this.$stateParams.namespace
    };
    this.myDatasetApi
      .list(params)
      .$promise
      .then(
        (datasets) => {
          datasets.forEach( (dataset) => { this.fetchDatasetTags(dataset.name); } );
        },
        () => { console.error('Error on fetching datasets tags '); }
      );
  }
  fetchDatasetTags(datasetId) {
    let params = {
      namespaceId: this.$stateParams.namespace,
      datasetId: datasetId
    };
    this.myTagsApi
      .getDatasetTags(params)
      .$promise
      .then(
        (datasetTags) => {
          this.checkAndMarkLoading(datasetTags.length);
          this.tags = this.tags.concat(datasetTags);
        },
        () => { console.error('Fetching tags for dataset failed: ', datasetId); }
      );
  }
  getStreamsTags() {
    let params = {
      namespace: this.$stateParams.namespace
    };
    this.myStreamApi
      .list(params)
      .$promise
      .then(
        (streams) => {  streams.forEach((stream) => { this.fetchStreamTags(stream.name); } ); },
        () => { console.error('Fetching streams tags failed'); }
      );
  }
  fetchStreamTags(streamId) {
    let params = {
      namespaceId: this.$stateParams.namespace,
      streamId: streamId
    };
    this.myTagsApi
      .getStreamTags(params)
      .$promise
      .then(
        (streamTags) => {
          this.checkAndMarkLoading(streamTags.length);
          this.tags = this.tags.concat(streamTags);
        },
        () => { console.error('Fetching stream tags failed for: ', streamId); }
      );
  }
  fetchProgramTags(appId, programId, programType) {
    let params = {
      namespaceId: this.$stateParams.namespace,
      appId: appId,
      programType: (['mapreduce', 'spark'].indexOf(programType) === -1)? programType + 's': programType,
      programId: programId
    };
    this.myTagsApi
      .getProgramTags(params)
      .$promise
      .then(
        (programTags) => {
          this.checkAndMarkLoading(programTags.length);
          this.tags = this.tags.concat(programTags);
        },
        () => { console.error('Fetching tags'); }
      );
  }
  checkAndMarkLoading(tagsCount) {
    if (tagsCount) {
      this.loadingData = false;
    }
  }
}

SearchController.$inject = ['myTagsApi', 'myAppsApi', 'myDatasetApi', 'myStreamApi', '$stateParams', '$state', 'caskFocusManager', '$scope'];

angular.module(`${PKG.name}.feature.search`)
  .controller('SearchController', SearchController);
