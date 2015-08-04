angular.module(PKG.name + '.feature.worker')
  .controller('WorkersRunsController', function($scope, $filter, $state, rRuns) {
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
       $scope.runs.selected = rRuns[0];
     }
   });

   $scope.tabs = [
   {
     title: 'Status',
     template: '/assets/features/workers/templates/tabs/runs/tabs/status.html'
   },
   {
     title: 'Logs',
     template: '/assets/features/workers/templates/tabs/runs/tabs/log.html'
   }];

   $scope.activeTab = $scope.tabs[0];

  $scope.selectTab = function(tab) {
    $scope.activeTab = tab;
  };
 });
