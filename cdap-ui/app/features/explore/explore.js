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
  .controller('GlobalExploreController', function ($scope, $state, EventPipe, myExploreApi) {

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

    myExploreApi.list(params)
      .$promise
      .then(function (res) {
        angular.forEach(res, function(v) {
          var split = v.table.split('_');
          v.type = split[0];
          split.splice(0,1); // removing the data type from the array
          v.name = split.join('_');
        });

        this.dataList = res;
        this.selectTable(res[0]);
      }.bind(this));

    EventPipe.on('explore.newQuery', function() {
      if (this.activePanel.indexOf(1) === -1) {
        this.activePanel = [0,1];
      }
    }.bind(this));

    this.selectTable = function (data) {
      // Passing this info to sql-query directive
      this.type = data.type;
      this.name = data.name;

      params.table = data.table;

      // Fetching info of the table
      myExploreApi.getInfo(params)
        .$promise
        .then(function (res) {
          this.selectedInfo = res;
        }.bind(this));

    };

  });
