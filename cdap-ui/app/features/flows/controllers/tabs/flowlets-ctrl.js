angular.module(PKG.name + '.feature.flows')
  .controller('FlowletsController', function($scope, MyDataSource, $state, $filter, FlowDiagramData) {
    var dataSrc = new MyDataSource($scope);
    var filterFilter = $filter('filter');

    $scope.flowlets = [];

    FlowDiagramData.fetchData($state.params.appId, $state.params.programId)
      .then(function (res) {
        angular.forEach(res.flowlets, function(v) {
          var name = v.flowletSpec.name;
          v.isOpen = false;
          $scope.flowlets.push({name: name, isOpen: $state.params.flowletid === name});
        });

        if (!$scope.$parent.activeFlowlet) {
          $scope.flowlets[0].isOpen = true;
          $scope.activeFlowlet = $scope.flowlets[0];
        } else {
          var match = filterFilter($scope.flowlets, {name: $scope.$parent.activeFlowlet});
          match[0].isOpen = true;
          $scope.activeFlowlet = match[0];
        }

      });


    $scope.selectFlowlet = function(flowlet) {
      angular.forEach($scope.flowlets, function(f) {
        f.isOpen = false;
      });

      flowlet.isOpen = true;

      $scope.activeFlowlet = flowlet;
    };

  });
