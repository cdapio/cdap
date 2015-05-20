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
            nodes = [],
            nodesFromBackend = angular.copy(res.nodes);

        // Add Start and End nodes as semantically workflow needs to have it.
        nodesFromBackend.unshift({
          type: 'START',
          nodeType: 'ACTION',
          nodeId: '',
          program: {
            programName: 'Start'
          }
        });

        nodesFromBackend.push({
          label: 'end',
          type: 'END',
          nodeType: 'ACTION',
          nodeId: '',
          program: {
            programName: 'End'
          }
        });

        expandNodes(nodesFromBackend, nodes);
        convertNodesToEdges(angular.copy(nodes), edges);

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

/**
  * Purpose: convertNodesToEdgess a list of nodes to a list of connections
  * @param  [Array] of nodes
  * @return [Array] of connections (edges)
  * Should handle Action + Fork + Condition Nodes.
*/
function convertNodesToEdges(nodes, connections) {
  var staticNodeTypes = [
   'ACTION', // from backend hence no 'NODE'
   'JOINNODE',
   'FORKNODE',
   'CONDITIONNODE',
   'CONDITIONEND'
 ];
  for (var i=0; i < nodes.length -1; i++) {

    if (staticNodeTypes.indexOf(nodes[i].nodeType) >-1 &&
        staticNodeTypes.indexOf(nodes[i+1].nodeType) > -1
      ) {
        if (nodes[i].nodeId === nodes[i+1].nodeId) {
          continue; // Don't connect the fork and join nodes of the same fork
        }
      connections.push({
        sourceName: nodes[i].program.programName + nodes[i].nodeId,
        targetName: nodes[i+1].program.programName + nodes[i+1].nodeId,
        sourceType: nodes[i].nodeType
      });
    } else if (nodes[i].nodeType === 'FORK' || nodes[i].nodeType === 'CONDITION') {
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
    convertNodesToEdges(temp, connections);
  }
}

/**
  Purpose: Expand a fork and convertNodesToEdges branched nodes to a list of connections
  * @param  [Array] of nodes
  * @return [Array] of connections

  * {nodeId}: will be used when constructing edges.

*/
function expandNodes(nodes, expandedNodes) {
  var i, j, nodeId;
  for(i=0; i<nodes.length; i++) {
    if (nodes[i].nodeType === 'ACTION') {
      nodes[i].label = nodes[i].program.programName;
      expandedNodes.push(nodes[i]);
    } else if (nodes[i].nodeType === 'FORK') {
      for (j=0; j<nodes[i].branches.length; j++) {
        expandedNodes.push({
          label: 'FORK',
          nodeType: 'FORKNODE',
          nodeId: 'FORK' + i,
          program: {
            programName: 'FORKNODE'
          }
        });
        expandNodes(nodes[i].branches[j], expandedNodes);
        expandedNodes.push({
          label: 'JOIN',
          nodeType: 'JOINNODE',
          nodeId: 'FORK' + i,
          program: {
            programName: 'JOINNODE'
          }
        });
      }
    } else if (nodes[i].nodeType === 'CONDITION') {
      nodes[i].branches = [nodes[i].ifBranch, nodes[i].elseBranch];
      for (j=0; j<nodes[i].branches.length; j++) {
        expandedNodes.push({
          label: 'IF',
          nodeType: 'CONDITIONNODE',
          nodeId: 'IF' + i,
          program: {
            programName: nodes[i].predicateClassName
          }
        });
        expandNodes(nodes[i].branches[j], expandedNodes);
        expandedNodes.push({
          label: 'ENDIF',
          nodeType: 'CONDITIONEND',
          nodeId: 'IF' + i,
          program: {
            programName: 'CONDITIONEND'
          }
        });
      }
    }
  }
}
