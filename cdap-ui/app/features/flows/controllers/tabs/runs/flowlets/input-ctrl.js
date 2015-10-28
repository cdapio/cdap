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

 class FlowletDetailInputController{
   constructor($state, $scope, MyCDAPDataSource, MyMetricsQueryHelper, MyChartHelpers, myFlowsApi) {
    this.dataSrc = new MyCDAPDataSource($scope);
    this.$state = $state;
    this.MyMetricsQueryHelper = MyMetricsQueryHelper;
    this.$scope = $scope;
    this.MyChartHelpers = MyChartHelpers;
    this.flowletid = $scope.FlowletsController.activeFlowlet.name;

    this.metric = {
     startTime: 'now-60s',
     endTime: 'now',
     resolution: '1s',
     names: ['system.queue.pending']
    };

    this.inputs = [];

    this.chartSettings = {
     chartMetadata: {
       showx: true,
       showy: true,
       legend: {
         show: false,
         position: 'inset'
       }
     },
     color: {
       pattern: ['red']
     },
     isLive: true,
     interval: 1000,
     aggregate: 5
    };

    let params = {
     namespace: this.$state.params.namespace,
     appId: this.$state.params.appId,
     flowId: this.$state.params.programId,
     scope: this.$scope
    };

    myFlowsApi.get(params)
     .$promise
     .then( (res) => {
       // INPUTS
       angular.forEach(res.connections, (v) => {
         if (v.targetName === this.flowletid) {
           this.inputs.push({
             name: v.sourceName,
             max: 0,
             type: v.sourceType
           });
         }
       });

       if (this.inputs.length > 0) {
         this.formatInput();
       }
     });

   }

   formatInput() {
    let arrivalPath;

    angular.forEach(this.inputs,  (input) => {
      let flowletTags = {
        namespace: this.$state.params.namespace,
        app: this.$state.params.appId,
        flow: this.$state.params.programId,
        consumer: this.flowletid,
        producer: input.name
      };

      var path = '/metrics/query?' + this.MyMetricsQueryHelper.tagsToParams(flowletTags) + '&metric=system.queue.pending';

      this.dataSrc.poll({
        _cdapPath: path + '&start=now-60s&end=now&aggregate=true',
        method: 'POST',
        interval: 2000
      },  (res) => {
        // Get initial aggregate
        let aggregate = res.series[0] ? res.series[0].data[0].value : 0;

        this.dataSrc.request({
          _cdapPath: '/metrics/query',
          method: 'POST',
          body: this.MyMetricsQueryHelper.constructQuery('qid', flowletTags, this.metric)
        })
        .then( (seriesResult) => {
          this.MyChartHelpers.formatTimeseries(aggregate, seriesResult, input, this.metric);
        });
      });

      arrivalPath = '/metrics/query?metric=system.process.events.processed'+
        '&tag=namespace:' + this.$state.params.namespace +
        '&tag=app:' + this.$state.params.appId +
        '&tag=flow' + this.$state.params.programId +
        '&tag=flowlet:' + this.flowletid +
        '&tag=run:' + this.$scope.RunsController.runs.selected.runid +
        '&start=now-1s&end=now';
       // TODO: should this value be averaged over more than just the past 1 second?
       // POLLING ARRIVAL RATE
      this.dataSrc
        .poll({
          _cdapPath: arrivalPath,
          method: 'POST'
        }, function (res) {
          if (res.series[0]) {
            input.total = res.series[0].data[0].value;
          }
        });
    });
  }
}

FlowletDetailInputController.$inject = ['$state', '$scope', 'MyCDAPDataSource', 'MyMetricsQueryHelper', 'MyChartHelpers', 'myFlowsApi'];

angular.module(`${PKG.name}.feature.flows`)
  .controller('FlowletDetailInputController', FlowletDetailInputController);
