angular.module(PKG.name + '.feature.admin')
  .controller('PreferencesController', function ($scope, $filter, $alert, $state, rSource, myPreferenceApi) {
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
      $scope.messages = 'Specify new or override system configurations that will be accessible in all applications & datasets within this namespace.';

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
      $scope.messages = 'Specify new or override namespace configurations that will be accessible in all programs within this application.';

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
          $alert({
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
