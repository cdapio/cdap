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

angular.module(PKG.name + '.feature.adapters')
  .controller('AdaptersDetailHistoryController', function($scope, AdapterDetail) {
    var params = {};
    angular.copy(AdapterDetail.params, params);
    params.scope = $scope;

    $scope.historyParams = {
      appId: params.appId,
      programId: params.workflowId || params.workerId
    };

    $scope.programType = AdapterDetail.programType;

    AdapterDetail.api.runs(params)
      .$promise
      .then(function (res) {
        $scope.runsHistory = res;
      });
  });
