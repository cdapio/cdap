angular.module(PKG.name + '.feature.spark')
  .controller('SparkRunsController', function($scope, $filter, $state, rRuns, $bootstrapModal, rSparkDetail) {
    var fFilter = $filter('filter'),
        match;
    this.runs = rRuns;
    this.$bootstrapModal = $bootstrapModal;
    this.description = rSparkDetail.description;
    if ($state.params.runid) {
      match = fFilter(rRuns, {runid: $state.params.runid});
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
      template: '/assets/features/spark/templates/tabs/runs/tabs/status.html'
    },
    {
      title: 'Logs',
      template: '/assets/features/spark/templates/tabs/runs/tabs/log.html'
    }];

    this.activeTab = this.tabs[0];

    this.selectTab = function(tab) {
      this.activeTab = tab;
    };

    this.openHistory = function() {
      this.$bootstrapModal.open({
        size: 'lg',
        template: '<my-program-history data-runs="runs" data-type="SPARK"></my-program-history>',
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
