class WorkflowsRunsController {
  constructor($scope, $state, $filter, rRuns) {
    let fFilter = $filter('filter');
    this.runs = rRuns;

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

    $scope.$watch(
      angular.bind(this, () => this.runs.selected.runid ),
      () => {
        if ($state.params.runid) {
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

    this.activeTab = this.tabs[0];
  }

  selectTab(tab) {
    this.activeTab = tab;
  }
}

WorkflowsRunsController.$inject = ['$scope', '$state', '$filter', 'rRuns'];

angular.module(`${PKG.name}.feature.workflows`)
  .controller('WorkflowsRunsController', WorkflowsRunsController);
