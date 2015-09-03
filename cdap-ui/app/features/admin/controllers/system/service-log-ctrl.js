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

angular.module(PKG.name + '.feature.admin').controller('SystemServiceLogController',
function ($scope, $state, MyDataSource, $timeout) {

    var myDataSrc = new MyDataSource($scope);

    var path = '/system/services/' + encodeURIComponent($state.params.serviceName) + '/logs/';

    $scope.loadingNext = true;
    myDataSrc.request({
      _cdapPath: path + 'next?&maxSize=50'
    })
      .then(function(response) {
        $scope.logs = response;
        $scope.loadingNext = false;
      });

    $scope.loadNextLogs = function () {
      if ($scope.loadingNext) {
        return;
      }

      $scope.loadingNext = true;
      var fromOffset = $scope.logs[$scope.logs.length-1].offset;

      myDataSrc.request({
        _cdapPath: path + 'next?&maxSize=50&fromOffset=' + fromOffset
      })
        .then(function (res) {
          $scope.logs = _.uniq($scope.logs.concat(res));
          $scope.loadingNext = false;
        });
    };

    $scope.loadPrevLogs = function () {
      if ($scope.loadingPrev) {
        return;
      }

      $scope.loadingPrev = true;
      var fromOffset = $scope.logs[0].offset;

      myDataSrc.request({
        _cdapPath: path + 'prev?maxSize=50&fromOffset=' + fromOffset
      })
        .then(function (res) {
          $scope.logs = _.uniq(res.concat($scope.logs));
          $scope.loadingPrev = false;

          $timeout(function() {
            document.getElementById(fromOffset).scrollIntoView();
          });
        });
    };

});
