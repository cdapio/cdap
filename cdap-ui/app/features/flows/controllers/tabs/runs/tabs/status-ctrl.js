/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

 class FlowsRunDetailStatusController {
   constructor($state, $scope, MyCDAPDataSource, myHelpers, FlowDiagramData, $timeout, MyMetricsQueryHelper, myFlowsApi) {
     this.dataSrc = new MyCDAPDataSource($scope);
     this.$state = $state;
     this.$scope = $scope;
     this.myHelpers = myHelpers;
     this.myFlowsApi = myFlowsApi;
     this.MyMetricsQueryHelper = MyMetricsQueryHelper;
     this.data = {};
     this.data.metrics = {};
     this.data.instances = {};
     this.onChangeFlag = 1;

    FlowDiagramData.fetchData($state.params.appId, $state.params.programId)
      .then( (data) => {
        this.data = data;
        this.data.metrics = {};
        this.data.instances = {};
        this.pollMetrics();
        this.onChangeFlag +=1;
      });
    $scope.$on('$destroy', FlowDiagramData.reset.bind(FlowDiagramData));
   }

   pollMetrics() {
    let nodes = this.data.nodes;
    var generateStreamMetricsPath = function (streamName) {
      let streamTags = {
        namespace: this.$state.params.namespace,
        stream: streamName
      };
      return '/metrics/query?metric=system.collect.events&aggregate=true&' + this.MyMetricsQueryHelper.tagsToParams(streamTags);
    };
    // Requesting Metrics data
    // We just need to poll for streams, individually, in a flow for processed events.
    // The flowlet metrics could be batched and made as single request.
    // There is a groupBy: ['flowlet'] but not 'stream'
    nodes.forEach( (node) => {
      if (node.type === 'STREAM') {
       this.dataSrc.poll({
         _cdapPath: generateStreamMetricsPath.call(this, node.name),
         method: 'POST',
         interval: 2000
       }, (data) => {
         this.data.metrics[node.name] = this.myHelpers.objectQuery(data, 'series' , 0, 'data', 0, 'value') || 0;
         this.onChangeFlag += 1;
       });
      } else {
        let params = {
          namespace: this.$state.params.namespace,
          appId: this.$state.params.appId,
          flowId: this.$state.params.programId,
          flowletId: node.name,
          scope: this.$scope
        };
        this.myFlowsApi.pollFlowletInstance(params)
          .$promise
          .then( (res) => {
            this.data.instances[node.name] = res.instances;
            this.onChangeFlag += 1;
          });
      }
    });

    // Batched query for flowlet metrics.
    this.dataSrc.poll(
    {
     _cdapPath: '/metrics/query',
     method: 'POST',
     body: this.MyMetricsQueryHelper.constructQuery(
       'qid',
       {
         namespace: this.$state.params.namespace,
         app: this.$state.params.appId,
         flow: this.$state.params.programId,
         run: this.$scope.RunsController.runs.selected.runid
       },
       {names: ['system.process.events.processed']},
       false,
       ['flowlet', 'stream']
     )
    }, (res) => {
     let dataSeries = res.qid.series;

     if (dataSeries.length) {
       dataSeries.forEach( (data) => {
         this.data.metrics[data.grouping.flowlet] = data.data[0].value;
       });
       this.onChangeFlag += 1;
     }
    });

  }

  flowletClick(node) {
    this.$scope.RunsController.selectTab(this.$scope.RunsController.tabs[1], node);
  }

}

FlowsRunDetailStatusController.$inject = ['$state', '$scope', 'MyCDAPDataSource', 'myHelpers', 'FlowDiagramData', '$timeout', 'MyMetricsQueryHelper', 'myFlowsApi'];

angular.module(`${PKG.name}.feature.flows`)
  .controller('FlowsRunDetailStatusController', FlowsRunDetailStatusController);
