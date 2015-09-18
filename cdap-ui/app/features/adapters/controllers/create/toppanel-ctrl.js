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

angular.module(PKG.name + '.feature.adapters')
  .controller('TopPanelController', function(EventPipe, CanvasFactory, MyAppDAGService, $scope, $timeout, $bootstrapModal, ModalConfirm, $alert, $state, $stateParams, GLOBALS, AdapterErrorFactory, MyConsoleTabService) {

    this.metadata = MyAppDAGService['metadata'];
    function resetMetadata() {
      this.metadata = MyAppDAGService['metadata'];
    }
    this.GLOBALS = GLOBALS;
    this.metadataExpanded = false;
    MyAppDAGService.registerResetCallBack(resetMetadata.bind(this));

    if ($stateParams.name) {
      this.metadata.name = $stateParams.name;
    }
    if ($stateParams.type) {
      if ([GLOBALS.etlBatch, GLOBALS.etlRealtime].indexOf($stateParams.type) !== -1) {
        this.metadata.template.type = $stateParams.type;
      } else {
        $alert({
          type: 'danger',
          content: 'Invalid template type. Has to be either ETLBatch or ETLRealtime'
        });
      }
    }
    this.saveMetadata = function() {
      this.metadata['name'] = this.pipelineName;
      this.metadata['description'] = this.pipelineDescription;
      this.metadataExpanded = false;
    };

    this.openMetadata = function () {
      this.metadata = MyAppDAGService['metadata'];
      if (this.metadataExpanded) { return; }
      EventPipe.emit('popovers.close');
      var name = this.metadata.name;
      var description = this.metadata.description;
      this.metadataExpanded = true;
      this.pipelineName = name;
      this.pipelineDescription = description;
    };

    this.resetMetadata = function() {
      this.metadata.name = this.pipelineName;
      this.metadata.description = this.pipelineDescription;
      this.metadataExpanded = false;
    };

    this.canvasOperations = [
      {
        name: 'Export'
      },
      {
        name: 'Save Draft'
      },
      {
        name: 'Validate'
      },
      {
        name: 'Publish'
      }
    ];

    this.onTopSideGroupItemClicked = function(group) {
      EventPipe.emit('popovers.close');
      var config;
      switch(group.name) {
        case 'Export':
          config = angular.copy(MyAppDAGService.getConfigForBackend());
          $bootstrapModal.open({
            templateUrl: '/assets/features/adapters/templates/create/popovers/viewconfig.html',
            size: 'lg',
            windowClass: 'cdap-modal',
            keyboard: true,
            controller: ['$scope', 'config', 'CanvasFactory', 'MyAppDAGService', function($scope, config, CanvasFactory, MyAppDAGService) {
              $scope.config = JSON.stringify(config);

              $scope.export = function () {
                CanvasFactory
                  .exportAdapter(
                    MyAppDAGService.getConfigForBackend(),
                    MyAppDAGService.metadata.name,
                    MyAppDAGService.nodes,
                    MyAppDAGService.connections)
                  .then(
                    function success(result) {
                      $scope.exportFileName = result.name;
                      $scope.url = result.url;
                      $scope.$on('$destroy', function () {
                        URL.revokeObjectURL($scope.url);
                      });
                      // Clicking on the hidden download button. #hack.
                      $timeout(function() {
                        document.getElementById('adapter-export-config-link').click();
                      });
                    }.bind(this),
                    function error() {
                      console.log('ERROR: ' + 'exporting adapter failed');
                    }
                  );
              };
            }],
            resolve: {
              config: function() {
                return config;
              }
            }
          });
          break;
        case 'Publish':
          MyAppDAGService
            .save()
            .then(
              function sucess(adapter) {
                $alert({
                  type: 'success',
                  content: adapter + ' successfully published.'
                });
                $state.go('adapters.list');
              },
              function error(errorObj) {
                console.info('ERROR!: ', errorObj);
              }.bind(this)
            );
          break;
        case 'Save Draft':
          MyAppDAGService
            .saveAsDraft()
            .then(
              function success() {
                MyConsoleTabService.addMessage({
                  type: 'success',
                  content: MyAppDAGService.metadata.name + ' successfully saved as draft.'
                });
              },
              function error() {
                console.info('Failed saving as draft');
              }
            );
          break;
        case 'Validate':
          this.validatePipeline();
          break;
      }
    };

    this.importFile = function(files) {
      CanvasFactory
        .importAdapter(files, MyAppDAGService.metadata.template.type)
        .then(
          MyAppDAGService.onImportSuccess.bind(MyAppDAGService),
          function error(errorEvent) {
            console.error('Upload config failed', errorEvent);
          }
        );
    };

    this.validatePipeline = function() {
      var errors = AdapterErrorFactory.isModelValid(MyAppDAGService.nodes, MyAppDAGService.connections, MyAppDAGService.metadata, MyAppDAGService.getConfig());
      if (angular.isObject(errors)) {
        MyAppDAGService.notifyError(errors);
      } else {
        MyConsoleTabService.addMessage({
          type: 'success',
          content: MyAppDAGService.metadata.name + ' is valid .'
        });
      }
    };
  });
