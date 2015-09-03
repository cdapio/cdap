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

angular.module(PKG.name + '.feature.admin')
  .controller('NamespaceDatasetsController', function ($scope, $stateParams, myStreamApi, myDatasetApi) {

    $scope.dataList = [];
    $scope.currentPage = 1;

    var params = {
      namespace: $stateParams.nsadmin,
      scope: $scope
    };

    myDatasetApi.list(params)
      .$promise
      .then(function (res) {
        $scope.dataList = $scope.dataList.concat(res);
      });

    myStreamApi.list(params)
      .$promise
      .then(function (res) {
        angular.forEach(res, function(r) {
          r.type = 'Stream';
        });
        $scope.dataList = $scope.dataList.concat(res);
      });

  });
