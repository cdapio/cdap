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

angular.module(PKG.name + '.services')
  .service('myPipelineExportModalService', function($uibModal) {
    this.show = function(config, exportConfig) {
      if (!config || !exportConfig) {
        return;
      }
      var modalInstance = $uibModal.open({
        templateUrl: 'pipeline-export-modal/pipeline-export-modal-template.html',
        size: 'lg',
        keyboard: true,
        animation: false,
        windowTopClass: 'node-config-modal hydrator-modal',
        controller: ['$scope', 'config', '$timeout', 'exportConfig', function($scope, config, $timeout, exportConfig) {
          var exportTimeout = null;

          $scope.config = JSON.stringify(config);
          $scope.export = function () {
            var blob = new Blob([JSON.stringify(exportConfig, null, 4)], { type: 'application/json'});
            $scope.url = URL.createObjectURL(blob);
            $scope.exportFileName = (exportConfig.name? exportConfig.name: 'noname') + '-' + exportConfig.artifact.name;
            $scope.$on('$destroy', function () {
              URL.revokeObjectURL($scope.url);
            });

            $timeout.cancel(exportTimeout);
            exportTimeout = $timeout(function() {
              document.getElementById('pipeline-export-config-link').click();
              modalInstance.dismiss();
            });
          };

          $scope.$on('$destroy', function() {
            $timeout.cancel(exportTimeout);
          });
        }],
        resolve: {
          config: function(){ return config; },
          exportConfig: function() { return exportConfig; }
        }
      });
    };
  });
