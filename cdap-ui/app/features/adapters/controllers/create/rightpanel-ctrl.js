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
  .controller('RightPanelController', function(EventPipe, CanvasFactory, MyAppDAGService, $scope, $timeout, $bootstrapModal, ModalConfirm, $alert, $state) {
    this.canvasOperations = [
      {
        name: 'Import'
      },
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
      // {
      //   name: 'Settings'
      // },
      // {
      //   name: 'Config'
      // }
    ];

    this.onRightSideGroupItemClicked = function(group) {
      EventPipe.emit('popovers.close');
      var config;
      switch(group.name) {
        case 'Export':
          CanvasFactory
            .exportAdapter(
              MyAppDAGService.getConfigForBackend(),
              MyAppDAGService.metadata.name,
              MyAppDAGService.nodes,
              MyAppDAGService.connections)
            .then(
              function success(result) {
                this.exportFileName = result.name;
                this.url = result.url;
                $scope.$on('$destroy', function () {
                  URL.revokeObjectURL(this.url);
                }.bind(this));
                // Clicking on the hidden download button. #hack.
                $timeout(function() {
                  document.getElementById('adapter-export-config-link').click();
                });
              }.bind(this),
              function error() {
                console.log('ERROR: ' + 'exporting adapter failed');
              }
            );
          break;
        case 'Import':
          // Clicking on the hidden upload button. #hack.
          $timeout(function() {
            document.getElementById('adapter-import-config-link').click();
          });
          break;
        case 'Config':
          config = angular.copy(MyAppDAGService.getConfigForBackend());
          $bootstrapModal.open({
            templateUrl: '/assets/features/adapters/templates/create/popovers/viewconfig.html',
            size: 'lg',
            windowClass: 'adapter-modal',
            keyboard: true,
            controller: ['$scope', 'config', function($scope, config) {
              $scope.config = JSON.stringify(config);
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
                $state.go('apps.list');
              },
              function error(errorObj) {
                console.info('ERROR!: ', errorObj);
              }.bind(this)
            );
          break;
        case 'Settings':

          MyAppDAGService.isConfigTouched = true;
          $bootstrapModal.open({
            templateUrl: '/assets/features/adapters/templates/create/popovers/settings.html',
            size: 'lg',
            windowClass: 'adapter-modal',
            keyboard: true,
            controller: ['$scope', 'metadata', 'EventPipe', function($scope, metadata, EventPipe) {
              $scope.metadata = metadata;
              var metadataCopy = angular.copy(metadata);
              $scope.reset = function() {
                $scope.metadata.template.schedule.cron = metadataCopy.template.schedule.cron;
                $scope.metadata.template.instance = metadataCopy.template.instance;
                EventPipe.emit('plugin.reset');
              };

              function closeFn() {
                $scope.reset();
                $scope.$close('cancel');
              }

              ModalConfirm.confirmModalAdapter(
                $scope,
                $scope.metadata,
                metadataCopy,
                closeFn
              );

            }],
            resolve: {
              'metadata': function() {
                return MyAppDAGService.metadata;
              }
            }
          });
          break;
        case 'Save Draft':
          MyAppDAGService
            .saveAsDraft()
            .then(
              function success() {
                $alert({
                  type: 'success',
                  content: MyAppDAGService.metadata.name + ' successfully saved as draft.'
                });
                $state.go('adapters.drafts');
              },
              function error() {
                console.info('Failed saving as draft');
              }
            );
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



  });
