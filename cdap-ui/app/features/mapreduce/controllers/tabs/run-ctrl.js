angular.module(PKG.name + '.feature.mapreduce')
  .controller('MapreduceRunsController', function($scope, $state, $rootScope, rRuns, $filter) {
    var fFilter = $filter('filter'),
        match;
    this.runs = rRuns;

    if ($state.params.runid) {
      match = fFilter(rRuns, {runid: $state.params.runid});
      if (match.length) {
        this.runs.selected = match[0];
      }
    } else if (rRuns.length) {
      this.runs.selected = rRuns[0];
    } else {
      this.runs.selected = {
        runid: 'No Runs!'
      };
    }

    $scope.$watch('runs.selected.runid', function() {
     if ($state.params.runid) {
       return;
     } else {
        if (rRuns.length) {
          this.runs.selected = rRuns[0];
        }
     }
   }.bind(this));

    this.tabs = [{
      title: 'Status',
      template: '/assets/features/mapreduce/templates/tabs/runs/tabs/status.html'
    },
    {
      title: 'Mappers',
      template: '/assets/features/mapreduce/templates/tabs/runs/tabs/mappers.html'
    },
    {
      title: 'Reducers',
      template: '/assets/features/mapreduce/templates/tabs/runs/tabs/reducers.html'
    },
    {
      title: 'Logs',
      template: '/assets/features/mapreduce/templates/tabs/runs/tabs/log.html'
    }];

    this.activeTab = this.tabs[0];

    this.selectTab = function(tab) {
      this.activeTab = tab;
    };

  });
