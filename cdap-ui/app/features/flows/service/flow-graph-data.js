angular.module(PKG.name + '.feature.flows')
  .service('FlowDiagramData', function($state, $q, myFlowsApi) {
    this.data = {};

    this.fetchData = function(appId, flowId) {
      var registeredFlows = Object.keys(this.data);
      if (registeredFlows.indexOf(flowId) > -1) {
        return $q.when(this.data[flowId]);
      }

      var defer = $q.defer();

      var params = {
        namespace: $state.params.namespace,
        appId: appId,
        flowId: flowId
      };

      myFlowsApi.get(params)
        .$promise
        .then(function (res) {
          var nodes = [];
          angular.forEach(res.connections, function(conn) {
            if (conn.sourceType === 'STREAM') {
              nodes.push({
                type: conn.sourceType,
                name: conn.sourceName
              });
            }
          });

          angular.forEach(res.flowlets, function (val, key) {
            val.type = 'FLOWLET';
            val.name = key;
            nodes.push(val);
          });

          this.data[flowId] = {
            flowlets: res.flowlets,
            nodes: nodes,
            edges: res.connections,
            metrics: {}
          };
          defer.resolve(this.data[flowId]);
        }.bind(this));


      return defer.promise;
    };

  });
