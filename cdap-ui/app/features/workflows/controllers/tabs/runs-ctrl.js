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

var params = {};
class WorkflowsRunsController {
  constructor($scope, $state, $filter, rRuns, myWorkFlowApi, $uibModal, rWorkflowDetail) {
    let fFilter = $filter('filter');
    this.runs = rRuns;
    this.$scope = $scope;
    this.$state = $state;
    this.myWorkFlowApi = myWorkFlowApi;
    this.runStatus = null;
    this.$uibModal = $uibModal;
    this.description = rWorkflowDetail.description;


    if ($state.params.runid) {
      var match = fFilter(rRuns, {runid: $state.params.runid});
      if (match.length) {
        this.runs.selected = angular.copy(match[0]);
      }
    } else if (rRuns.length) {
      this.runs.selected = angular.copy(rRuns[0]);
    } else {
      this.runs.selected = {
        runid: 'No Runs'
      };
    }

    if (rRuns.length) {
      this.startPollingCurrentRun();
    }

    $scope.$watch(
      angular.bind(this, () => this.runs.selected.runid ),
      () => {
        if (this.$state.params.runid) {
          params['runId'] = this.runs.selected.runid;
          this.myWorkFlowApi
              .stopPollRunDetail(params)
              .$promise
              .then(this.startPollingCurrentRun.bind(this));
          return;
        } else {
          if (rRuns.length) {
            this.runs.selected = angular.copy(rRuns[0]);
          }
        }

      }
    );

    this.tabs = [
      {
        title: 'Status',
        template: '/old_assets/features/workflows/templates/tabs/runs/tabs/status.html'
      },
      {
        title: 'Logs',
        template: '/old_assets/features/workflows/templates/tabs/runs/tabs/log.html'
      }
    ];

    $scope.$on('$destroy', () => {
      this.myWorkFlowApi.stopPollRunDetail(params);
    });
    this.activeTab = this.tabs[0];

    this.metadataParams = {
      namespace: $state.params.namespace,
      appId: $state.params.appId,
      programType: 'workflows',
      programId: $state.params.programId,
      scope: $scope
    };

  }

  startPollingCurrentRun() {
    params = {
      namespace: this.$state.params.namespace,
      appId: this.$state.params.appId,
      workflowId: this.$state.params.programId,
      scope: this.$scope,
      runId: this.runs.selected.runid
    };
    this.myWorkFlowApi
      .pollRunDetail(params)
      .$promise
      .then( response => {
        this.runStatus = response.status;
      });
  }

  selectTab(tab) {
    this.activeTab = tab;
  }

  stop() {
    this.runStatus = 'STOPPING';
    this.myWorkFlowApi
     .stopRun(params, {})
     .$promise
     .then( () => {
       this.$state.go(this.$state.current, this.$state.params, {reload: true});
     });
  }

  suspend() {
    this.runStatus = 'SUSPENDING';
    this.myWorkFlowApi
     .suspendRun(params, {})
     .$promise
     .then( () => {
       this.$state.go(this.$state.current, this.$state.params, {reload: true});
     });
  }

  resume() {
    this.runStatus = 'RESUMING';
    this.myWorkFlowApi
     .resumeRun(params, {})
     .$promise
     .then( () => {
       this.$state.go(this.$state.current, this.$state.params, {reload: true});
     });
  }

  openHistory() {
    this.$uibModal.open({
      size: 'lg',
      templateUrl: '/old_assets/features/workflows/templates/tabs/history.html',
      controller: ['runs', '$scope', function(runs, $scope) {
        $scope.runs = runs;
      }],
      resolve: {
        runs:() => this.runs
      }
    });
  }

  openSchedules() {
    this.$uibModal.open({
      size: 'lg',
      templateUrl: '/old_assets/features/workflows/templates/tabs/schedules.html',
      controller: 'WorkflowsSchedulesController',
      controllerAs: 'SchedulesController'
    });
  }

}

WorkflowsRunsController.$inject = ['$scope', '$state', '$filter', 'rRuns', 'myWorkFlowApi', '$uibModal', 'rWorkflowDetail'];

angular.module(`${PKG.name}.feature.workflows`)
  .controller('WorkflowsRunsController', WorkflowsRunsController);
