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
  .controller('PreferencesController', function ($scope, $filter, $state, rSource, myPreferenceApi, myAlertOnValium) {
    var filterFilter = $filter('filter');

    $scope.parentPreferences = [];
    $scope.preferences = [];

    /*
      Start Data Modelling Implementation
    */
    var getPreference,
      params,
      parentPreference,
      parentParams,
      setPreference;

    if (rSource === 'SYSTEM') {
      $scope.heading = 'System Preferences';

      params = {
        scope: $scope
      };
      getPreference = myPreferenceApi.getSystemPreference;
      setPreference = myPreferenceApi.setSystemPreference;

    } else if (rSource === 'NAMESPACE') {
      $scope.heading = 'Namespace Preferences: ' + $state.params.nsadmin;
      $scope.messages = 'Specify new or override existing system configurations that will be accessible in all applications and datasets within this namespace';

      parentParams = {
        scope: $scope,
        resolved: true
      };
      parentPreference = myPreferenceApi.getSystemPreference;

      params = {
        namespace: $state.params.nsadmin,
        scope: $scope
      };
      getPreference = myPreferenceApi.getNamespacePreference;
      setPreference = myPreferenceApi.setNamespacePreference;

    } else if (rSource === 'APPLICATION') {
      $scope.heading = 'Application Preferences: ' + $state.params.appId;
      $scope.messages = 'Specify new or override existing namespace configurations that will be accessible in all programs within this application';

      parentParams = {
        namespace: $state.params.nsadmin,
        scope: $scope,
        resolved: true
      };
      parentPreference = myPreferenceApi.getNamespacePreference;

      params = {
        namespace: $state.params.nsadmin,
        appId: $state.params.appId,
        scope: $scope
      };
      getPreference = myPreferenceApi.getAppPreference;
      setPreference = myPreferenceApi.setAppPreference;

    }

    $scope.preferences = [];

    $scope.loadProperties = function () {
      if (rSource !== 'SYSTEM') {
        parentPreference(parentParams)
          .$promise
          .then(function (res) {
            $scope.parentPreferences = formatObj(res);
          });
      }

      getPreference(params)
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
      var match = filterFilter($scope.preferences, preference);
      if (match.length) {
        $scope.preferences.splice($scope.preferences.indexOf(match[0]), 1);
      }
    };

    $scope.save = function() {
      var obj = {};

      angular.forEach($scope.preferences, function(v) {
        if (v.key) {
          obj[v.key] = v.value;
        }
      });

      setPreference(params, obj,
        function () {
          myAlertOnValium.show({
            content: 'Your preferences have been successfully saved',
            type: 'success'
          });
          $scope.loadProperties();
        });
    };

    $scope.enter = function (event, last) {
      if (last && event.keyCode === 13) {
        $scope.addPreference();
      } else {
        return;
      }
    };

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

  });
