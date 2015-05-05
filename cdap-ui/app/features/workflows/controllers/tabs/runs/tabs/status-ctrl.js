angular.module(PKG.name + '.feature.workflows')
  .controller('WorkflowsRunsStatusController', function($state, $scope, MyDataSource, $filter, $alert) {
    var dataSrc = new MyDataSource($scope),
        filterFilter = $filter('filter'),
        basePath = '/apps/' + $state.params.appId + '/workflows/' + $state.params.programId;
    if ($state.params.runid) {
      var match = filterFilter($scope.runs, {runid: $state.params.runid});
      if (match.length) {
        $scope.runs.selected = match[0];
      }
    }

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


    $scope.workflowProgramClick = function (instance) {
      if (['START', 'END'].indexOf(instance.type) > -1) {
        return;
      }
      if ($scope.runs.length) {
        if (instance.program.programType === 'MAPREDUCE') {
          $state.go('mapreduce.detail.runs.run', {
            programId: instance.program.programName,
            runid: $scope.runs.selected.properties[instance.nodeId]
          });
        }
      } else {
        $alert({
          type: 'info',
          content: 'No runs for the workflow: '+ $state.params.programId +' yet.'
        });
      }
    };

    $scope.stop = function() {
      $alert({
        type: 'info',
        content: 'Stopping a workflow at run level is not possible yet. Will be fixed soon.'
      });
      return;
      $scope.status = 'STOPPING';
      dataSrc.request({
        _cdapNsPath: basePath + '/stop',
        method: 'POST'
      });
    };

    $scope.goToDetailActionView = function(programId, programType) {
      // As of 2.7 only a mapreduce job is scheduled in a workflow.
      if (programType === 'MAPREDUCE') {
        $state.go('mapreduce.detail', {
          programId: programId
        });
      }
    };


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
