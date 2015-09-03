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
  .controller('AdapterRunDetailLogController', function($scope, MyDataSource, $state, $q) {
    var dataSrc = new MyDataSource($scope),
        logPath = '';

    if (!$scope.runs.length) {
      return;
    }

    $scope.logs = [];

    // TODO: The logs will get handled differently from the backend. Will change
    dataSrc.request({
     _cdapPath: '/namespaces/' + $state.params.namespace +
                '/adapters/' + $state.params.adapterId
    })
    .then(function(res) {
      var appId = res.program.application;
      var programId = res.program.id;
      logPath = '/apps/' + appId +
                '/'+ res.program.type.toLowerCase() + 's' +'/' + programId +
                '/runs/' + $scope.runs.selected.runid +
                '/logs/next?max=50';
      return $q.when(logPath);
    })
      .then(function(lpath) {
        dataSrc.poll({
          _cdapNsPath: lpath
        }, function(res) {
          $scope.logs = res;
        });
      });


  });
