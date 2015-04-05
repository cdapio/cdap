angular.module(PKG.name + '.feature.flows')
  .controller('FlowletsController', function($scope, MyDataSource, $state, $rootScope) {
    var dataSrc = new MyDataSource($scope);

    $scope.flowlets = [];

    dataSrc
      .request({
        _cdapNsPath: '/apps/' + $state.params.appId+  '/flows/' + $state.params.programId
      })
      .then(function (res) {
        angular.forEach(res.flowlets, function(v) {
          var name = v.flowletSpec.name;
          $scope.flowlets.push({name: name, isOpen: $state.params.flowletid === name});

        });
      });



    // This is for toggling (opening/closing) accordions if state changes.
    $rootScope.$on('$stateChangeSuccess', function(event, toState, toParams, fromState, fromParams) {
      if (fromState.name !== 'flows.detail.flowlets.flowlet' && toState.name !== 'flows.detail.flowlets.flowlet') {
        return;
      }
      angular.forEach($scope.flowlets, function(value) {
        console.log('test');
        if (value.name === toParams.flowletid) {
          value.isOpen = true;
        }
        if (value.name === fromParams.flowletid) {
          value.isOpen = false;
        }
      });
    });

  });