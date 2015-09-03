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
  .controller('myFlowController', function($scope, myHelpers) {
    function update(newVal) {
      // Avoid rendering the graph without nodes and edges.
      if (myHelpers.objectQuery(newVal, 'nodes') && myHelpers.objectQuery(newVal, 'edges')) {
        $scope.render();
      }
    }

    $scope.instanceMap = {};
    $scope.labelMap = {};

    // This is done because of performance reasons.
    // Earlier we used to have scope.$watch('model', function, true); which becomes slow with large set of
    // nodes. So the controller/component that is using this directive need to pass in this flag and update it
    // whenever there is a change in the model. This way the watch becomes smaller.

    // The ideal solution would be to use a service and have this directive register a callback to the service.
    // Once the service updates the data it could call the callbacks by updating them with data. This way there
    // is no watch. This is done in adapters and we should fix this ASAP.
    $scope.$watch('onChangeFlag', function(newVal) {
      if (newVal) {
        update($scope.model);
      }
    });

  });
