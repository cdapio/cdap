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

class TrackerPreviewController {
  constructor($state, myTrackerApi, $scope, MyCDAPDataSource) {
    this.$state = $state;
    this.myTrackerApi = myTrackerApi;
    this.$scope = $scope;
    this.dataSrc = new MyCDAPDataSource($scope);
    this.loading = true;
    this.viewLimit = 10;

    this.entityExploreUrl = window.getAbsUIUrl({
      namespaceId: $state.params.namespace,
      entityType: $state.params.entityType,
      entityId: $state.params.entityId
    });
    this.entityExploreUrl += '?modalToOpen=explore';
    this.generatePreview();
  }

  generatePreview() {
    let params = {
      namespace: this.$state.params.namespace,
      scope: this.$scope
    };

    let entityType;
    if (this.$state.params.entityType === 'streams') {
      entityType = 'stream';
    } else if (this.$state.params.entityType === 'datasets') {
      entityType = 'dataset';
    }

    let query = 'SELECT * FROM ' + entityType + '_' + this.$state.params.entityId + ' LIMIT 500';

    this.myTrackerApi.postQuery(params, { query: query })
      .$promise
      .then((res) => {
        this.pollQueryStatus(res.handle);
      });
  }

  pollQueryStatus(handle) {
    let queryPath = '/data/explore/queries/' + handle + '/status';

    this.dataSrc.poll({
      _cdapPath: queryPath,
      interval: 2000
    }, (res) => {
      if (res.status === 'FINISHED') {
        this.dataSrc.stopPoll(res.__pollId__);
        this.fetchQueryResults(handle);
      }
    });
  }

  fetchQueryResults(handle) {
    let params = {
      handle: handle,
      scope: this.$scope
    };

    this.myTrackerApi.getQuerySchema(params)
      .$promise
      .then((res) => {
        angular.forEach(res, function(column) {
          // check for '.' in the name, before splitting on it, because in the case that specific columns are
          // queried, the column names in the schema are not prefixed by the dataset name
          if (column.name.indexOf('.') !== -1) {
            column.name = column.name.split('.')[1];
          }
        });
        this.previewSchema = res;
      });

    this.myTrackerApi.getQueryResults(params, { size: 500 })
      .$promise
      .then((res) => {
        this.previewData = res;
        this.loading = false;
      });
  }

  loadNextData() {
    this.viewLimit += 10;
  }
}

TrackerPreviewController.$inject = ['$state', 'myTrackerApi', '$scope','MyCDAPDataSource'];

angular.module(PKG.name + '.feature.tracker')
 .controller('TrackerPreviewController', TrackerPreviewController);
