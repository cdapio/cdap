/*
 * Copyright © 2015-2017 Cask Data, Inc.
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
        config: '=',
        datasetType: '@',
        stageName: '='
      },
      templateUrl: 'widget-container/widget-dataset-selector/widget-dataset-selector.html',
      controller: function ($scope, myDatasetApi, myStreamApi, $state, EventPipe, $uibModal) {

        $scope.textPlaceholder = '';
        if ($scope.config['widget-attributes'] && $scope.config['widget-attributes']['placeholder']) {
          $scope.textPlaceholder = $scope.config['widget-attributes']['placeholder'];
        }

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
        // This variable is to make sure that when the name of the dataset is changed
        // from a non-existing dataset to another non-existing one, then the schema is
        // not updated. However, the schema will be updated when changing from a non-existing
        // schema to an existing one, or vice versa
        var isCurrentlyExistingDataset;
        var initialized = false;
        var schema;
        var oldDataset;
        var newDataset;
        var modalOpen = false;

        var showPopupFunc = function(schema, oldDatasetName) {
          let sinkName = $scope.stageName;
          let confirmModal = $uibModal.open({
              templateUrl: '/assets/features/hydrator/templates/create/popovers/change-dataset-confirmation.html',
              size: 'lg',
              backdrop: 'static',
              keyboard: false,
              windowTopClass: 'confirm-modal hydrator-modal center',
              controller: ['$scope', function($scope) {
                $scope.datasetName = params.datasetId;
                $scope.sinkName = sinkName;
              }]
            });
          modalOpen = true;

          confirmModal.result.then((confirm) => {
            if (confirm) {
              isCurrentlyExistingDataset = true;

              if (!schema) {
                $scope.schemaError = true;
                EventPipe.emit('schema.clear');
              } else {
                $scope.schemaError = false;
                EventPipe.emit('dataset.selected', schema, null, true, $scope.model);
              }
            } else {
              $scope.model = oldDatasetName;
            }
            modalOpen = false;
          });
        };

        var debouncedPopup = _.debounce(showPopupFunc, 1500);

        resource.list(params)
          .$promise
          .then(function (res) {
            $scope.list = res;

            dataMap = res.map(function (d) { return d.name; });
            if (dataMap.indexOf($scope.model) === -1 ) {
              isCurrentlyExistingDataset = false;
            } else {
              isCurrentlyExistingDataset = true;
            }
          });

        $scope.showConfirmationModal = function() {
          if (oldDataset !== newDataset) {
            params.datasetId = newDataset;
            resource.get(params)
              .$promise
              .then(function (res) {
                schema = res.spec.properties.schema;

                if (!isCurrentlyExistingDataset && dataMap.indexOf(newDataset) !== -1) {
                  if (debouncedPopup) {
                    debouncedPopup.cancel();
                  }
                  showPopupFunc(schema, oldDataset);
                }
              });
          }
        };

        $scope.$watch('model', function (newDatasetName, oldDatasetName) {
          oldDataset = oldDatasetName;
          newDataset = newDatasetName;
          $scope.schemaError = false;

          if (debouncedPopup) {
            debouncedPopup.cancel();
          }
          if (dataMap.length === 0) {
            initialized = true;
          }
          if (isCurrentlyExistingDataset && dataMap.length > 0 && dataMap.indexOf($scope.model) === -1) {
            EventPipe.emit('dataset.selected', '', null, false);
            isCurrentlyExistingDataset = false;
            return;
          }

          if ($scope.datasetType === 'stream') {
            params.streamId = $scope.model;
          } else if ($scope.datasetType === 'dataset') {
            params.datasetId = $scope.model;
          }

          resource.get(params)
            .$promise
            .then(function (res) {
              if ($scope.datasetType === 'stream') {
                schema = JSON.stringify(res.format.schema);
                var format = res.format.name;
                EventPipe.emit('dataset.selected', schema, format);

              } else if ($scope.datasetType === 'dataset') {
                  schema = res.spec.properties.schema;

                  if (initialized && !isCurrentlyExistingDataset && newDataset !== oldDataset) {
                    if (!modalOpen) {
                      debouncedPopup(schema, oldDataset);
                    }
                  } else {
                    initialized = true;

                    if (schema) {
                      EventPipe.emit('dataset.selected', schema, null, true, $scope.model);
                    }
                  }
                }
            });
        });


        $scope.$on('$destroy', function () {
          EventPipe.cancelEvent('dataset.selected');
          if (debouncedPopup) {
            debouncedPopup.cancel();
          }
        });

      }
    };
  });
