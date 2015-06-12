angular.module(PKG.name + '.feature.flows')
  .controller('FlowsRunDetailStatusController', function($state, $scope, MyDataSource, myHelpers, FlowDiagramData, $timeout, MyMetricsQueryHelper, myFlowsApi) {
    var dataSrc = new MyDataSource($scope),
        basePath = '/apps/' + $state.params.appId + '/flows/' + $state.params.programId;

    this.data = {};

    FlowDiagramData.fetchData($state.params.appId, $state.params.programId)
      .then(function(data) {
        this.data = data;
        pollMetrics.bind(this)();
      }.bind(this));

    function generateStreamMetricsPath(streamName) {
      var streamTags = {
        namespace: $state.params.namespace,
        stream: streamName
      };
      return '/metrics/query?metric=system.collect.events&aggregate=true&' + MyMetricsQueryHelper.tagsToParams(streamTags);
    }

    function generateFlowletMetricsPath(flowletName) {
      var flowletTags = {
        namespace: $state.params.namespace,
        app: $state.params.appId,
        flow: $state.params.programId,
        run: $scope.RunsController.runs.selected.runid,
        flowlet: flowletName
      };
      return '/metrics/query?metric=system.process.events.processed&aggregate=true&' + MyMetricsQueryHelper.tagsToParams(flowletTags);
    }

    function pollMetrics() {
      var nodes = this.data.nodes;
      this.data.instances = {};
      // Requesting Metrics data
      angular.forEach(nodes, function (node) {
        if (node.type !== 'STREAM' && !$scope.RunsController.runs.length) {
          return;
        }
        dataSrc.poll({
          _cdapPath: (node.type === 'STREAM' ? generateStreamMetricsPath(node.name): generateFlowletMetricsPath(node.name)),
          method: 'POST'
        }, function (data) {
          this.data.metrics[node.name] = myHelpers.objectQuery(data, 'series' , 0, 'data', 0, 'value') || 0;
        }.bind(this));

        // Polling for Flowlet Instance
        if (node.type !== 'STREAM') {
          var params = {
            namespace: $state.params.namespace,
            appId: $state.params.appId,
            flowId: $state.params.programId,
            flowletId: node.name,
            scope: $scope
          };
          myFlowsApi.pollFlowletInstance(params)
            .$promise
            .then(function (res) {
              this.data.instances[node.name] = res.instances;
            }.bind(this));
        }

      }.bind(this));
    }

    this.flowletClick = function(node) {
      $scope.RunsController.selectTab($scope.RunsController.tabs[1], node);
    };
  });
