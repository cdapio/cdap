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

angular.module(PKG.name + '.feature.datasets')
  .controller('DatasetExploreController',
    function($state, explorableDatasets, $filter) {
      var filterFilter = $filter('filter');
      var datasetId = $state.params.datasetId;
      datasetId = datasetId.replace(/[\.\-]/g, '_');
      var match = filterFilter(explorableDatasets, datasetId);
      if (match.length) {
        match = match[0];
        this.tableName = match.table;
        this.databaseName = match.database;
        this.datasetName = datasetId;
      }

    }
  );
