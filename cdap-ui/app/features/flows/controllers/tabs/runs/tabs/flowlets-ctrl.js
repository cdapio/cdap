angular.module(PKG.name + '.feature.flows')
  .controller('FlowletsController', function($scope, $state, $filter, FlowDiagramData) {
    var filterFilter = $filter('filter');

    this.flowlets = [];

    FlowDiagramData.fetchData($state.params.appId, $state.params.programId)
      .then(function (res) {
        angular.forEach(res.flowlets, function(v) {
          var name = v.flowletSpec.name;
          v.isOpen = false;
          this.flowlets.push({name: name, isOpen: $state.params.flowletid === name});
        }.bind(this));

        if (!$scope.RunsController.activeFlowlet) {
          this.flowlets[0].isOpen = true;
          this.activeFlowlet = this.flowlets[0];
        } else {
          var match = filterFilter(this.flowlets, {name: $scope.RunsController.activeFlowlet});
          match[0].isOpen = true;
          this.activeFlowlet = match[0];
        }

      }.bind(this));


    this.selectFlowlet = function(flowlet) {
      angular.forEach(this.flowlets, function(f) {
        f.isOpen = false;
      });
      var match = filterFilter(this.flowlets, flowlet);

      match[0].isOpen = true;

      this.activeFlowlet = match[0];
    };

  });
