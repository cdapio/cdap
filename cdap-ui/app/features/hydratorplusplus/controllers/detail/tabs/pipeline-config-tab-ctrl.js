/*
 * Copyright © 2016 Cask Data, Inc.
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

angular.module(PKG.name + '.feature.hydratorplusplus')
  .controller('HydratorPlusPlusDetailConfigCtrl', function(HydratorPlusPlusDetailNonRunsStore, $timeout, $scope) {
    this.setState = function() {
      this.config = HydratorPlusPlusDetailNonRunsStore.getConfigJson();
    };
    this.setState();

    this.exportConfig = function () {
      var exportConfigJson = angular.copy(HydratorPlusPlusDetailNonRunsStore.getCloneConfig());
      var blob = new Blob([JSON.stringify(exportConfigJson, null, 4)], { type: 'application/json'});
      this.url = URL.createObjectURL(blob);
      this.exportFileName = (exportConfigJson.name ? exportConfigJson.name : 'noname') + '-' + exportConfigJson.artifact.name;
      $scope.$on('$destroy', function () {
        URL.revokeObjectURL(this.url);
      }.bind(this));
      $timeout(function() {
        document.getElementById('pipeline-export-config-link').click();
      });
    };

    HydratorPlusPlusDetailNonRunsStore.registerOnChangeListener(this.setState.bind(this));
  });
