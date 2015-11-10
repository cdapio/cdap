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

angular.module(PKG.name + '.commons')
  .directive('myProgramStatus', function () {

    return {
      restrict: 'E',
      scope: {
        type: '@',
        runid: '=',
        pollInterval: '@',
        runs: '='
      },
      templateUrl: 'program-status/program-status.html',
      controller: function($scope, MyCDAPDataSource, $state) {
        // $scope.runs comes from parent controller
        if ($scope.runs.length !== 0) {
          var dataSrc = new MyCDAPDataSource($scope),
              path = '';
          var runCheck;
          var pollPromise;
          var runMetadata = function (newVal) {
            if (runCheck === newVal) {
              return;
            }
            runCheck = newVal;
            if (pollPromise && pollPromise.__pollId__) {
              dataSrc.stopPoll(pollPromise.__pollId__);
            }

            path = '/apps/' + $state.params.appId + '/' + $scope.type + '/' + $state.params.programId + '/runs/' + $scope.runid;

            pollPromise = dataSrc.poll({
              _cdapNsPath: path,
              interval: $scope.pollInterval || 10000
            }, function (res) {
              var start = res.start;
              $scope.start = new Date(start*1000);
              $scope.status = res.status;
              $scope.duration = (res.end ? (res.end) - start : 0);
              if (['COMPLETED', 'KILLED', 'STOPPED', 'FAILED'].indexOf($scope.status) !== -1) {
                dataSrc.stopPoll(pollPromise.__pollId__);
              }
            });
          };

          runMetadata();
          $scope.$watch('runid', runMetadata);
        }

      }
    };
  });
