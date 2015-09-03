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
  .controller('AdapterRunsController', function($scope, $filter, $state, rRuns) {
  var fFilter = $filter('filter');
  $scope.runs = rRuns;

   if ($state.params.runid) {
     var match = fFilter(rRuns, {runid: $state.params.runid});
     if (match.length) {
       $scope.runs.selected = match[0];
     }
   } else if (rRuns.length) {
     $scope.runs.selected = rRuns[0];
   } else {
     $scope.runs.selected = {
       runid: 'No Runs'
     };
   }

   $scope.$watch('runs.selected.runid', function() {
     if ($state.params.runid) {
       return;
     } else {
       if (rRuns.length) {
         $scope.runs.selected = rRuns[0];
       }
     }
   });

   $scope.tabs = [
   {
     title: 'Status',
     template: '/assets/features/adapters/templates/tabs/runs/tabs/status.html'
   }, {
     title: 'Log',
     template: '/assets/features/adapters/templates/tabs/runs/tabs/log.html'
   }];

   $scope.activeTab = $scope.tabs[0];

   $scope.selectTab = function(tab, node) {
    if (tab.title === 'Flowlets') {
      $scope.activeFlowlet = node;
    }
    $scope.activeTab = tab;

   };
 });
