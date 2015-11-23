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

class FlowDiagramData {
  constructor($state, $q, myFlowsApi) {
    this.data = {};
    this.$state = $state;
    this.$q = $q;
    this.myFlowsApi = myFlowsApi;
  }

  fetchData(appId, flowId) {
    var registeredFlows = Object.keys(this.data);
    if (registeredFlows.indexOf(flowId) > -1) {
      return this.$q.when(this.data[flowId]);
    }

    var defer = this.$q.defer();

    var params = {
      namespace: this.$state.params.namespace,
      appId: appId,
      flowId: flowId
    };

    this.myFlowsApi.get(params)
      .$promise
      .then( res => {
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
      });


    return defer.promise;
  }
  reset() {
    this.data = {};
  }
}
FlowDiagramData.$inject = ['$state', '$q', 'myFlowsApi'];
angular.module(`${PKG.name}.feature.flows`)
  .service('FlowDiagramData', FlowDiagramData);
