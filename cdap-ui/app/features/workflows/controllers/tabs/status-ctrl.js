angular.module(PKG.name + '.feature.workflows')
  .controller('WorkflowsStatusController', function($scope, MyDataSource, $state) {
      var dataSrc = new MyDataSource($scope),
          basePath = '/apps/' + $state.params.appId + '/workflows/' + $state.params.programId;
      $scope.status = null;
      $scope.duration = null;
      $scope.startTime = null;
      $scope.data = {};
      dataSrc.request({
        _cdapNsPath: basePath
      })
        .then(function(res) {
          var edges = [],
              nodes = [];

          res.nodes.unshift({
            name: 'start',
            type: 'START',
            nodeType: 'ACTION',
            nodeId: 'start',
            program: {
              programName: ''
            }
          });

          res.nodes.push({
            name: 'end',
            type: 'END',
            nodeType: 'ACTION',
            nodeId: 'end',
            program: {
              programName: ''
            }
          });
          convert(angular.copy(res.nodes), edges);
          expandForks(res.nodes, nodes);

          nodes = nodes.map(function(item) {
            return angular.extend({
              name: item.program.programName + item.nodeId,
              type: item.nodeType
            }, item);
          });

          $scope.data = {
            nodes: nodes,
            edges: edges,
            metrics: {}
          };

          var programs = [];
          angular.forEach(res.nodes, function(value) {
            programs.push(value.program);
          });
          $scope.actions = programs;
        });
    });

  function convert(nodes, connections) {

    for (var i=0; i < nodes.length -1; i++) {

      if (nodes[i].nodeType === 'ACTION' && nodes[i+1].nodeType === 'ACTION') {
        connections.push({
          sourceName: nodes[i].program.programName + nodes[i].nodeId,
          targetName: nodes[i+1].program.programName + nodes[i+1].nodeId,
          sourceType: nodes[i].nodeType
        });
      } else if (nodes[i].nodeType === 'FORK') {
        flatten(nodes[i-1], nodes[i], nodes[i+1], connections);
      }

    }
  }

  /**
    * Purpose: Flatten a source-fork-target combo to a list of connections
    * @param  [Array] of nodes
    * @param  [Array] of nodes
    * @param  [Array] of nodes
    * @return [Array] of connections
    */
  function flatten(source, fork, target, connections) {
    var branches = fork.branches,
        temp = [];

    for (var i =0; i<branches.length; i++) {
      temp = branches[i];
      if(source) {
        temp.unshift(source);
      }
      if(target) {
        temp.push(target);
      }
      convert(temp, connections);
    }
  }

  /**
    Purpose: Expand a fork and convert branched nodes to a list of connections
    * @param  [Array] of nodes
    * @return [Array] of connections
    */
  function expandForks(nodes, expandedNodes) {
    for(var i=0; i<nodes.length; i++) {
      if (nodes[i].nodeType === 'ACTION') {
        expandedNodes.push(nodes[i]);
      } else if (nodes[i].nodeType === 'FORK') {
        for (var j=0; j<nodes[i].branches.length; j++) {
          expandForks(nodes[i].branches[j], expandedNodes);
        }
      }
    }
  }
