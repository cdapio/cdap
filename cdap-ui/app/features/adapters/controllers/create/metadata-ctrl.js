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
  .controller('MetadataController', function(MyAppDAGService, rConfig, $stateParams, $alert, EventPipe, $bootstrapModal, ModalConfirm) {
    this.metadata = MyAppDAGService['metadata'];
    function resetMetadata() {
      this.metadata = MyAppDAGService['metadata'];
    }

    MyAppDAGService.registerResetCallBack(resetMetadata.bind(this));

    if ($stateParams.name) {
      this.metadata.name = $stateParams.name;
    }
    if ($stateParams.type) {
      if (['ETLBatch', 'ETLRealtime'].indexOf($stateParams.type) !== -1) {
        this.metadata.template.type = $stateParams.type;
      } else {
        $alert({
          type: 'danger',
          content: 'Invalid template type. Has to be either ETLBatch or ETLRealtime'
        });
      }
    }

    this.showMetadataModal = function() {
      EventPipe.emit('popovers.close');

      if (this.metadata.error) {
        delete this.metadata.error;
      }
      MyAppDAGService.isConfigTouched = true;
      $bootstrapModal
        .open({
          templateUrl: '/assets/features/adapters/templates/create/popovers/metadata-detail.html',
          size: 'lg',
          windowClass: 'adapter-modal',
          keyboard: true,
          controller: ['$scope', 'metadata', function($scope, metadata) {
            $scope.modelCopy = angular.copy(this.metadata);
            $scope.metadata = metadata;
            $scope.reset = function () {
              metadata['name'] = $scope.modelCopy.name;
              metadata['description'] = $scope.modelCopy.description;
            }.bind(this);

            function closeFn() {
              $scope.reset();
              $scope.$close('cancel');
            }

            ModalConfirm.confirmModalAdapter(
              $scope,
              $scope.metadata,
              $scope.modelCopy,
              closeFn
            );


          }.bind(this)],
          resolve: {
            metadata: function() {
              return this['metadata'];
            }.bind(this)
          }
        })
        .result
        .finally(function() {
          MyAppDAGService.metadata.name = this.metadata.name;
          MyAppDAGService.metadata.description = this.metadata.description;
        }.bind(this));
    };

  });
