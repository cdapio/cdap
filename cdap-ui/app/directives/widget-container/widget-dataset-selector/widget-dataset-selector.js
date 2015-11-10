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

angular.module(PKG.name + '.commons')
  .directive('myDatasetSelector', function () {
    return {
      restrict: 'E',
      scope: {
        model: '=ngModel',
        datasetType: '@'
      },
      templateUrl: 'widget-container/widget-dataset-selector/widget-dataset-selector.html',
      controller: function ($scope, myDatasetApi, myStreamApi, $state, EventPipe) {
        var resource;
        $scope.list = [];

        if ($scope.datasetType === 'stream') {
          resource = myStreamApi;
        } else if ($scope.datasetType === 'dataset') {
          resource = myDatasetApi;
        }

        var params = {
          namespace: $state.params.namespace || $state.params.nsadmin
        };

        var dataMap = [];

        resource.list(params)
          .$promise
          .then(function (res) {
            $scope.list = res;

            dataMap = res.map(function (d) { return d.name; });
          });

        $scope.$watch('model', function () {
          if (!$scope.model || dataMap.indexOf($scope.model) === -1 ) { return; }

          if ($scope.datasetType === 'stream') {
            params.streamId = $scope.model;
          } else if ($scope.datasetType === 'dataset') {
            params.datasetId = $scope.model;
          }

          resource.get(params)
            .$promise
            .then(function (res) {
              var schema;

              if ($scope.datasetType === 'stream') {
                schema = JSON.stringify(res.format.schema);
                var format = res.format.name;

                EventPipe.emit('dataset.selected', schema, format);
              } else if ($scope.datasetType === 'dataset') {
                schema = res.spec.properties.schema;
                EventPipe.emit('dataset.selected', schema);
              }
            });
        });


        $scope.$on('$destroy', function () {
          EventPipe.cancelEvent('dataset.selected');
        });

      }
    };
  });
