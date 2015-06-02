angular.module(PKG.name + '.feature.workflows')
  .controller('WorkflowsRunsStatusController', function($state, $scope, myWorkFlowApi, $filter, $alert, GraphHelpers, MyDataSource) {
    var filterFilter = $filter('filter'),
        params = {
          appId: $state.params.appId,
          workflowId: $state.params.programId,
          scope: $scope
        };

    if ($state.params.runid) {
      var match = filterFilter($scope.runs, {runid: $state.params.runid});
      if (match.length) {
        $scope.runs.selected = match[0];
      }
    }

    $scope.data = {};
    myWorkFlowApi.get(params)
      .$promise
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

        GraphHelpers.expandNodes(nodesFromBackend, nodes);
        GraphHelpers.convertNodesToEdges(angular.copy(nodes), edges);

        nodes = nodes.map(function(item) {
          return angular.extend({
            name: item.program.programName + item.nodeId,
            type: item.nodeType
          }, item);
        });

        $scope.data = {
          nodes: nodes,
          edges: edges,
          metrics: {},
          current: {},
        };

        var programs = [];
        angular.forEach(res.nodes, function(value) {
          programs.push(value.program);
        });
        $scope.actions = programs;
      });

    // Still using MyDataSource because the poll needs to be stopped
    var dataSrc = new MyDataSource($scope);

    var path = '/apps/' + $state.params.appId
      + '/workflows/' + $state.params.programId
      + '/runs/' + $scope.runs.selected.runid;

    var runsParams = {
      appId: $state.params.appId,
      workflowId: $state.params.programId,
      runid: $scope.runs.selected.runid,
      scope: $scope
    };

    if ($scope.runs.length > 0) {
      dataSrc.poll({
        _cdapNsPath: path,
        interval: 1000
      })
      .then(function (response) {

        var pastNodes = Object.keys(response.properties);

        var activeNodes = filterFilter($scope.data.nodes , function(node) {
          return pastNodes.indexOf(node.nodeId) !== -1;
        });

        angular.forEach(activeNodes, function(n) {
          var runid = response.properties[n.nodeId];

          dataSrc.request({
            _cdapNsPath: '/apps/' + $state.params.appId +
              '/mapreduce/' + n.program.programName +
              '/runs/' + runid
          })
          .then(function (result) {
            $scope.data.current[n.name] = result.status;
          });
        });

        return response;
      })
      .then(function (response) {
        if (response.status === 'COMPLETED') {
          dataSrc.stopPoll(response.__pollId__);
        }
      });
    }


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
      myWorkFlowApi.stop(params);
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
