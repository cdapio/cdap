// FIXME: Needs to be re-thought.
var runparams = {},
    alertpromise,
    params;

class WorkflowsRunsStatusController {
  constructor($state, $scope, myWorkFlowApi, $filter, $alert, GraphHelpers, MyDataSource, myMapreduceApi, mySparkApi) {
    this.dataSrc = new MyDataSource($scope);
    this.$state = $state;
    this.$scope = $scope;
    this.myWorkFlowApi = myWorkFlowApi;
    this.$alert = $alert;
    this.GraphHelpers = GraphHelpers;
    this.myMapreduceApi = myMapreduceApi;
    this.mySparkApi = mySparkApi;
    this.$filter = $filter;
    this.runsCtrl = $scope.RunsController;

    params = {
      appId: this.$state.params.appId,
      workflowId: this.$state.params.programId,
      scope: this.$scope
    };

    this.runStatus = null;
    this.data = {};

    this.myWorkFlowApi.get(params)
      .$promise
      .then( res => {
        var edges = [],
            nodes = [],
            nodesFromBackend = angular.copy(res.nodes);

        this.GraphHelpers.addStartAndEnd(nodesFromBackend);
        this.GraphHelpers.expandNodes(nodesFromBackend, nodes);
        this.GraphHelpers.convertNodesToEdges(angular.copy(nodes), edges);

        nodes = nodes.map( item => {
          return angular.extend({
            name: item.program.programName + item.nodeId,
            type: item.nodeType
          }, item);
        });

        this.data = {
          nodes,
          edges,
          metrics: {},
          current: {},
        };

        var programs = [];
        angular.forEach(res.nodes, value => programs.push(value.program));

        this.actions = programs;

        // This needs to be rethought.
        this.pollNodes();

      });
  }

  // Need to make sure that the list of nodes is already generated
  pollNodes() {

    if (this.runsCtrl.runs.length === 0) {
      return;
    }
    runparams = angular.extend(
      {
        runId: this.runsCtrl.runs.selected.runid
      },
      params
    );

    this.myWorkFlowApi
      .pollRunDetail(runparams)
      .$promise
      .then( response => {
        this.runStatus = response.status;

        var pastNodes = Object.keys(response.properties);
        this.runsCtrl.runs.selected.properties = response.properties;

        var activeNodes = this.$filter('filter')(this.data.nodes , node => pastNodes.indexOf(node.nodeId) !== -1);

        angular.forEach(activeNodes, node => {
          var runid = response.properties[node.nodeId];
          var mapreduceParams;

          if (node.program.programType === 'MAPREDUCE') {
            mapreduceParams = {
              namespace: this.$state.params.namespace,
              appId: this.$state.params.appId,
              mapreduceId: node.program.programName,
              runId: runid,
              scope: this.$scope
            };
            this.myMapreduceApi.runDetail(mapreduceParams)
              .$promise
              .then( result => this.data.current[node.name] = result.status);
          } else if (node.program.programType === 'SPARK') {

            var sparkParams = {
              namespace: this.$state.params.namespace,
              appId: this.$state.params.appId,
              sparkId: node.program.programName,
              runId: runid,
              scope: this.$scope
            };

            this.mySparkApi.runDetail(sparkParams)
              .$promise
              .then( (result) => this.data.current[node.name] = result.status );
          }
        });

        if (['STOPPED', 'KILLED', 'COMPLETED'].indexOf(this.runStatus) !== -1) {
          this.myWorkFlowApi.stopPollRunDetail(runparams);
        }

      });

  }

  workflowProgramClick(instance) {
    if (['START', 'END'].indexOf(instance.type) > -1 ) {
      return;
    }
    if (this.runsCtrl.runs.length) {
      let stateParams = {
        programId: instance.program.programName,
        runid: this.runsCtrl.runs.selected.properties[instance.nodeId],
        sourceId: this.$state.params.programId,
        sourceRunId: this.$scope.RunsController.runs.selected.runid
      };
      if (instance.program.programType === 'MAPREDUCE' &&
         this.runsCtrl.runs.selected.properties[instance.nodeId]
        ) {
        stateParams.destinationType = 'Mapreduce';
        this.$state.go('mapreduce.detail.runs.run', stateParams);

      } else if (instance.program.programType === 'SPARK' &&
                this.runsCtrl.runs.selected.properties[instance.nodeId]
               ) {

        stateParams.destinationType = 'Spark';
        this.$state.go('spark.detail.runs.run', stateParams);
      }
    } else {
      var errorObj = {
        type: 'info',
        scope: this.$scope,
        content: `No runs for the workflow: ${this.$state.params.programId} yet.`
      }, e;
      if (!alertpromise) {
        alertpromise = this.$alert(errorObj);
        e = this.$scope.$on('alert.hide',() => {
          alertpromise = null;
          e(); // un-register from listening to the hide event of a closed alert.
        });
      }
    }
  }

  stop() {
    this.runStatus = 'STOPPING';
    this.myWorkFlowApi
     .stopRun(runparams, {});
  }

  suspend() {
    this.runStatus = 'SUSPENDING';
    this.myWorkFlowApi
     .suspendRun(runparams, {});
  }

  resume() {
    this.runStatus = 'RESUMING';
    this.myWorkFlowApi
     .resumeRun(runparams, {});
  }

}

WorkflowsRunsStatusController.$inject = ['$state', '$scope', 'myWorkFlowApi', '$filter', '$alert', 'GraphHelpers', 'MyDataSource', 'myMapreduceApi', 'mySparkApi'];
angular.module(`${PKG.name}.feature.workflows`)
  .controller('WorkflowsRunsStatusController', WorkflowsRunsStatusController);
