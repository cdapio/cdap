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
  .controller('ProgramPreferencesController', function($scope, $state, myPreferenceApi) {

    $scope.heading = $state.params.programId + ' Preferences';
    $scope.preferences = [];

    var params = {
      namespace: $state.params.namespace,
      appId: $state.params.appId,
      programType: $scope.type,
      programId: $state.params.programId,
      scope: $scope
    };

    $scope.loadProperties = function () {
      loadParentPreference();

      myPreferenceApi.getProgramPreference(params)
        .$promise
        .then(function (res) {
          $scope.preferences = formatObj(res);
        });
    };
    $scope.loadProperties();

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

      myPreferenceApi.setProgramPreference(params, obj,
        function () {
          $scope.loadProperties();
          $scope.$close(obj);
        });
    };

    $scope.enter = function (event, last) {
      if (last && event.keyCode === 13) {
        $scope.addPreference();
      } else {
        return;
      }
    };

    function loadParentPreference() {
      var parentParams = {
        namespace: $state.params.namespace,
        appId: $state.params.appId,
        scope: $scope,
        resolved: true
      };

      myPreferenceApi.getAppPreference(parentParams)
        .$promise
        .then(function (res) {
          $scope.systemPreferences = formatObj(res);
        });
    }

    function formatObj(input) {
      var arr = [];

      angular.forEach(JSON.parse(angular.toJson(input)), function(v, k) {
        arr.push({
          key: k,
          value: v
        });
      });

      return arr;
    }

  }) // end of controller

  .directive('myProgramPreferences', function() {
    return {
      restrict: 'E',
      controller: 'ProgramPreferencesController',
      templateUrl: 'program-preferences/program-preferences.html'
    };
  })
  .service('myProgramPreferencesService', function($bootstrapModal, $rootScope){
    var modalInstance;

    this.show = function(type) {

      var scope = $rootScope.$new();
      scope.type = type;

      modalInstance = $bootstrapModal.open({
        template: '<my-program-preferences></my-program-preferences>',
        size: 'lg',
        scope: scope
      });
      return modalInstance;
    };

  });
