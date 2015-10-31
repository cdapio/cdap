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


angular.module(PKG.name + '.feature.hydrator')
  .controller('NodeConfigController', function(NodeConfigStore, $scope, $timeout, $state) {
    this.setState = function() {
      $scope.isValidPlugin = false;
      $timeout(function() {
        var nodeState = NodeConfigStore.getState();
        $scope.plugin = nodeState.plugin;
        $scope.isSource = nodeState.isSource;
        $scope.isSink = nodeState.isSink;
        $scope.isTransform = nodeState.isTransform;
        $scope.isValidPlugin = nodeState.isValidPlugin;
        $scope.type = $state.params.type;
      });
    };
    NodeConfigStore.registerOnChangeListener(this.setState.bind(this));
  });
