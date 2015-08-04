var params = {};
class WorkflowsRunsController {
  constructor($scope, $state, $filter, rRuns, myWorkFlowApi) {
    let fFilter = $filter('filter');
    this.runs = rRuns;
    this.$scope = $scope;
    this.$state = $state;
    this.myWorkFlowApi = myWorkFlowApi;
    this.runStatus = null;

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
      startPollingCurrentRun.call(this);
    }

    function startPollingCurrentRun() {
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

    $scope.$watch(
      angular.bind(this, () => this.runs.selected.runid ),
      () => {
        if ($state.params.runid) {
          params['runId'] = this.runs.selected.runid;
          this.myWorkFlowApi
              .stopPollRunDetail(params)
              .$promise
              .then(startPollingCurrentRun.bind(this));
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
        template: '/assets/features/workflows/templates/tabs/runs/tabs/status.html'
      },
      {
        title: 'Logs',
        template: '/assets/features/workflows/templates/tabs/runs/tabs/log.html'
      }
    ];

    $scope.$on('$destroy', () => {
      this.myWorkFlowApi.stopPollRunDetail(params);
    });
    this.activeTab = this.tabs[0];
  }

  selectTab(tab) {
    this.activeTab = tab;
  }

  stop() {
    this.runStatus = 'STOPPING';
    this.myWorkFlowApi
     .stopRun(params, {});
  }

  suspend() {
    this.runStatus = 'SUSPENDING';
    this.myWorkFlowApi
     .suspendRun(params, {});
  }

  resume() {
    this.runStatus = 'RESUMING';
    this.myWorkFlowApi
     .resumeRun(params, {});
  }

}

WorkflowsRunsController.$inject = ['$scope', '$state', '$filter', 'rRuns', 'myWorkFlowApi'];

angular.module(`${PKG.name}.feature.workflows`)
  .controller('WorkflowsRunsController', WorkflowsRunsController);
