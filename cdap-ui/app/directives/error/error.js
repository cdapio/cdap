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

/**
 * myError
 */

angular.module(PKG.name+'.commons')

.controller('myErrorController', function($scope, myAlert) {
  $scope.alerts = myAlert.getAlerts();

  $scope.clear = function () {
    myAlert.clear();
    $scope.alerts = myAlert.getAlerts();
  };

  $scope.remove = function (item) {
    myAlert.remove(item);
  };
})

.directive('myError',
function myErrorDirective () {
  return {
    restrict: 'E',
    templateUrl: 'error/error-template.html',
    controller: function($scope, myAlert) {
      $scope.emptyError = function() {
        return myAlert.isEmpty();
      };
      $scope.errorCount = myAlert.count;
    }
  };

});
