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

angular.module(PKG.name + '.feature.flows')
  .controller('FlowletsController', function($scope, $state, $filter, FlowDiagramData) {
    var filterFilter = $filter('filter');

    this.flowlets = [];

    FlowDiagramData.fetchData($state.params.appId, $state.params.programId)
      .then(function (res) {
        angular.forEach(res.flowlets, function(v) {
          var name = v.flowletSpec.name;
          v.isOpen = false;
          this.flowlets.push({name: name, isOpen: $state.params.flowletid === name});
        }.bind(this));

        if (!$scope.RunsController.activeFlowlet) {
          this.flowlets[0].isOpen = true;
          this.activeFlowlet = this.flowlets[0];
        } else {
          var match = filterFilter(this.flowlets, {name: $scope.RunsController.activeFlowlet});
          match[0].isOpen = true;
          this.activeFlowlet = match[0];
        }

      }.bind(this));


    this.selectFlowlet = function(flowlet) {
      angular.forEach(this.flowlets, function(f) {
        f.isOpen = false;
      });
      var match = filterFilter(this.flowlets, flowlet);

      match[0].isOpen = true;

      this.activeFlowlet = match[0];
    };

  });
