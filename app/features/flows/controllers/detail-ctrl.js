angular.module(PKG.name + '.feature.flows')
  .controller('FlowsDetailController', function($scope, $state, $timeout) {
    if ($state.includes('**.runs.**')) {
      // If going to flow detail run tab don't do anything.
      // ui-router already has everything to load the state.
      return;
    } else if (!$state.params.runId && $state.includes('flows.detail.*')) {
      // If going to any state other than run tab just proceed.
      $timeout(function() {
        $state.go($state.current);
      });
    } else {
      // If coming from paren default to the run tab.
      $timeout(function() {
        $state.go('flows.detail.runs');
      })
    }
});
