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

angular.module(PKG.name + '.feature.explore')
  .controller('GlobalExploreController', function ($scope, $state, EventPipe, myExploreApi, $q, myDatasetApi) {

    this.activeTab = 0;

    this.activePanel = [0];
    this.openGeneral = true;
    this.openSchema = false;
    this.openPartition = false;

    this.dataList = []; // combined datasets and streams

    var params = {
      namespace: $state.params.namespace,
      scope: $scope
    };

    $q.all([myDatasetApi.list(params).$promise, myExploreApi.list(params).$promise])
      .then(function (res) {
        var exploreTables = res[1];
        var datasetsSpec = res[0];
        datasetsSpec = datasetsSpec
        .filter(dSpec => dSpec.properties['explore.database.name'] || dSpec.properties['explore.table.name'])
        .map(dSpec => {
          return {
            datasetName: dSpec.name,
            database: dSpec.properties['explore.database.name'] || 'default',
            table: dSpec.properties['explore.table.name'] || '',
            type: dSpec.type
          };
        });

        let tables = exploreTables.map(tb => {
          let tableIndex = tb.table.indexOf('_');
          let dbIndex = tb.database.indexOf('_');
          let matchingSpec = datasetsSpec.find(dspec => {
            let isSameTable = (dspec.table || '').toLowerCase() === (tableIndex !== -1 ? tb.table.slice(tableIndex) : tb.table);
            let isSameDB = (dspec.database || '').toLowerCase() === (dbIndex !== -1 ? tb.database.slice(dbIndex) : tb.database);
            return isSameTable || isSameDB;
          });
          if (matchingSpec) {
            let matchingSpecIndex = _.findIndex(datasetsSpec, matchingSpec);
            datasetsSpec.splice(matchingSpecIndex, 1);
            return {
              table: matchingSpec.table || tb.table,
              database: matchingSpec.database || tb.database,
              type: matchingSpec.type || '',
              datasetName: matchingSpec.datasetName
            };
          }
          if (tableIndex === -1) {
            tb.type = 'dataset';
          } else {
            var split = tb.table.split('_');
            tb.type = split[0];
          }
          return tb;
        });
        if (datasetsSpec.length) {
          tables = tables.concat(datasetsSpec);
        }
        this.dataList = tables;
        this.selectTable(this.dataList[0]);
      }.bind(this));
    EventPipe.on('explore.newQuery', function() {
      if (this.activePanel.indexOf(1) === -1) {
        this.activePanel = [0,1];
      }
    }.bind(this));

    this.selectTable = function (data) {
      // Passing this info to sql-query directive
      this.type = data.type;
      this.selectedTableName = data.table;
      this.selectedDatabaseName = data.database;
      this.selectedDatasetName = data.datasetName || data.name;

      params.table = data.table;

      // Fetching info of the table
      myExploreApi.getInfo(params)
        .$promise
        .then(function (res) {
          this.selectedInfo = res;
        }.bind(this));

    };

  });
