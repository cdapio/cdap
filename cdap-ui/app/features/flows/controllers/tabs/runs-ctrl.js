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

 class FlowsRunsController {
   constructor($scope, $filter, $state, rRuns, $bootstrapModal, rFlowsDetail) {
    this.$scope = $scope;
    this.$filter = $filter;
    this.runs = rRuns;
    this.$bootstrapModal = $bootstrapModal;
    this.rFlowsDetail = rFlowsDetail;
    this.description = rFlowsDetail.description;

    let fFilter = $filter('filter');
    if ($state.params.runid) {
      let match = fFilter(this.runs, {runid: $state.params.runid});
      if (match.length) {
        this.runs.selected = angular.copy(match[0]);
      } else {
        // Wrong runid. 404.
        $state.go('404');
        return;
      }
    } else if (this.runs.length) {
      this.runs.selected = angular.copy(this.runs[0]);
    } else {
      this.runs.selected = {
        runid: 'No Runs'
      };
    }

    this.$scope.$watch( () => {
      return this.runs.selected.runid;
    }, () => {
       if ($state.params.runid) {
         return;
       } else {
         if (this.runs.length) {
           this.runs.selected = angular.copy(this.runs[0]);
         }
       }
    });

    this.tabs = [
      {
        title: 'Status',
        template: '/assets/features/flows/templates/tabs/runs/tabs/status.html'
      },
      {
       title: 'Flowlets',
       template: '/assets/features/flows/templates/tabs/runs/tabs/flowlets.html'
      },
      {
        title: 'Logs',
        template: '/assets/features/flows/templates/tabs/runs/tabs/log.html'
      },
      {
        title: 'Datasets',
        template: '/assets/features/flows/templates/tabs/data.html'
      }
    ];

    this.activeTab = this.tabs[0];

    this.metadataParams = {
      namespace: $state.params.namespace,
      appId: $state.params.appId,
      programType: 'flows',
      programId: $state.params.programId,
      scope: this.$scope
    };

   }
   selectTab(tab, node) {
     if (tab.title === 'Flowlets') {
       this.activeFlowlet = node;
     }
     this.activeTab = tab;
   }

   openHistory() {
     this.$bootstrapModal.open({
       size: 'lg',
       templateUrl: '/assets/features/flows/templates/tabs/history.html',
       controller: ['runs', '$scope', function(runs, $scope) {
         $scope.runs = runs;
       }],
       resolve: {
         runs: () => {
           return this.runs;
         }
       }
     });
   }
 }

FlowsRunsController.$inject = ['$scope', '$filter', '$state', 'rRuns', '$bootstrapModal', 'rFlowsDetail'];

angular.module(PKG.name + '.feature.flows')
  .controller('FlowsRunsController', FlowsRunsController);
