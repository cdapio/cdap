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

class FlowsFlowletDetailController {
  constructor($state, $scope, myHelpers, myFlowsApi) {
    this.activeTab = 0;
    let flowletid = $scope.FlowletsController.activeFlowlet.name;
    this.myFlowsApi = myFlowsApi;

    this.params = {
      namespace: $state.params.namespace,
      appId: $state.params.appId,
      flowId: $state.params.programId,
      scope: $scope
    };

    myFlowsApi.get(this.params)
      .$promise
      .then( (res)=> {
        this.description = myHelpers.objectQuery(res, 'flowlets', flowletid, 'flowletSpec', 'description');
      });

    this.params.flowletId = flowletid;

    myFlowsApi.getFlowletInstance(this.params)
      .$promise
      .then( (res) =>{
        this.provisionedInstances = res.instances;
        this.instance = res.instances;
      });

    myFlowsApi.pollFlowletInstance(this.params)
      .$promise
      .then(function (res) {
        this.provisionedInstances = res.instances;
      }.bind(this));
  }
  setInstance() {
    this.myFlowsApi.setFlowletInstance(this.params, { 'instances': this.instance });
  }
}

FlowsFlowletDetailController.$inject = ['$state', '$scope', 'myHelpers', 'myFlowsApi'];
angular.module(`${PKG.name}.feature.flows`)
  .controller('FlowsFlowletDetailController', FlowsFlowletDetailController);
