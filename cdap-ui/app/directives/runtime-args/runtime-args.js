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

angular.module(PKG.name+'.commons')

  .directive('myRuntimeArgs', function() {
    return {
      restrict: 'E',
      controller: 'RuntimeArgumentsController',
      templateUrl: 'runtime-args/runtime-args.html'
    };
  })
  .service('myRuntimeService', function($bootstrapModal, $rootScope){
    var modalInstance;

    this.show = function(runtimeargs) {

      var scope = $rootScope.$new();
      scope.preferences = [];
      angular.forEach(runtimeargs, function(value, key) {
        scope.preferences.push({
          key: key,
          value: value
        });
      });

      modalInstance = $bootstrapModal.open({
        template: '<my-runtime-args></my-runtime-args>',
        size: 'lg',
        scope: scope
      });
      return modalInstance;
    };

  })
  .controller('RuntimeArgumentsController', function($scope) {

    $scope.preferences = $scope.preferences || [];

    $scope.addPreference = function() {
      $scope.preferences.push({
        key: '',
        value: ''
      });
    };

    $scope.removePreference = function(preference) {
      $scope.preferences.splice($scope.preferences.indexOf(preference), 1);
    };

    $scope.save = function() {
      var obj = {};

      angular.forEach($scope.preferences, function(v) {
        if (v.key) {
          obj[v.key] = v.value;
        }
      });

      $scope.$close(obj);
    };

  });
