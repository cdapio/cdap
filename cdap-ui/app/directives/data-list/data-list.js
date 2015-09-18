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

angular.module(PKG.name+'.commons')
.directive('myDataList', function myDataListDirective () {
  return {
    restrict: 'E',
    scope: {
      level: '@',
      program: '@',
      isHydrator: '='
    },
    templateUrl: 'data-list/data-list.html',
    controller: function($scope, MyDataSource, $state, myAdapterApi) {

      $scope.dataList = [];

      if (!$scope.isHydrator || $scope.level !== 'app') {
        nonHydrator();
      } else {
        hydrator();
      }

      function hydrator() {
        myAdapterApi.get({
          namespace: $state.params.namespace,
          adapter: $state.params.appId,
          scope: $scope
        })
          .$promise
          .then(function (res) {
            $scope.dataList = $scope.dataList.concat(
              res.datasets.map(function (dataset) {
                dataset.type = 'Dataset';
                return dataset;
              }),
              res.streams.map(function (stream) {
                stream.type = 'Stream';
                return stream;
              })
            );
          });
      }


      function nonHydrator() {
        var dataSrc = new MyDataSource($scope);

        var path = '/apps/' + $state.params.appId;

        if ($scope.level !== 'app') {
          switch ($scope.program) {
            case 'mapreduce':
              path += '/mapreduce/';
              break;
            case 'flow':
              path += '/flows/';
              break;
            case 'service':
              path += '/services/';
              break;
          }

          path += $state.params.programId;
        }



        // get datasets
        dataSrc.request({
          _cdapNsPath: path + '/datasets'
        }).then(function(res) {
          angular.forEach(res, function(d) {
            $scope.dataList.push({
              name: d.instanceId,
              type: 'Dataset'
            });
          });
        });

        // get streams
        dataSrc.request({
          _cdapNsPath: path + '/streams'
        }).then(function(res) {
          angular.forEach(res, function(s) {
            $scope.dataList.push({
              name: s.streamName,
              type: 'Stream'
            });
          });
        });

      }

    }
  };

});
