angular.module(PKG.name + '.feature.worker')
  .controller('WorkersRunsController', function($scope, $filter, $state, rRuns, $bootstrapModal, rWorkerDetail) {
  var fFilter = $filter('filter');

  this.runs = rRuns;
  this.description = rWorkerDetail.description;
  this.$bootstrapModal = $bootstrapModal;

  if ($state.params.runid) {
    var match = fFilter(rRuns, {runid: $state.params.runid});
    if (match.length) {
      this.runs.selected = angular.copy(match[0]);
    } else {
      $state.go('404');
      return;
    }
  } else if (rRuns.length) {
    this.runs.selected = angular.copy(rRuns[0]);
  } else {
    this.runs.selected = {
      runid: 'No Runs'
    };
  }

  $scope.$watch(angular.bind(this, function() {
    return this.runs.selected.runid;
  }), function() {
    if ($state.params.runid) {
      return;
    } else {
      if (rRuns.length) {
        this.runs.selected = angular.copy(rRuns[0]);
      }
    }
  }.bind(this));

  this.tabs = [
    {
      title: 'Status',
      template: '/assets/features/workers/templates/tabs/runs/tabs/status.html'
    },
    {
      title: 'Logs',
      template: '/assets/features/workers/templates/tabs/runs/tabs/log.html'
    }
  ];

  this.activeTab = this.tabs[0];

  this.selectTab = function(tab) {
    this.activeTab = tab;
  };

  this.openHistory = function() {
    this.$bootstrapModal.open({
      size: 'lg',
      windowClass: 'center cdap-modal',
      templateUrl: '/assets/features/workers/templates/tabs/history.html',
      controller: ['runs', '$scope', function(runs, $scope) {
        $scope.runs = runs;
      }],
      resolve: {
        runs: function() {
          return this.runs;
        }.bind(this)
      }
    });
  };
 });
