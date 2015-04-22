angular.module(PKG.name + '.feature.flows')
  .controller('FlowsRunsController', function($scope, $filter, $state, rRuns) {
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
       runid: 'No Runs!'
     }
   }

   $scope.$watch('runs.selected.runid', function(newVal) {
     if ($state.params.runid) {
       return;
     } else {
       $scope.runs.selected = rRuns[0];
     }
   })

   $scope.tabs = [
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
   }];

   $scope.activeTab = $scope.tabs[0];

   $scope.selectTab = function(tab, node) {
    if (tab.title === 'Flowlets') {
      $scope.activeFlowlet = node;
    }
    $scope.activeTab = tab;

   };
 });
