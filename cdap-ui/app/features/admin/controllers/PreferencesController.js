angular.module(PKG.name + '.feature.admin')
  .controller('PreferencesController', function ($scope, $filter, MyDataSource, $alert, $state, rSource) {
    var dataSrc = new MyDataSource($scope);
    var filterFilter = $filter('filter');

    var path = '';
    var parentPath = ''; // to fetch resolved preferences of parent

    if (rSource === 'SYSTEM') {
      path = '/preferences';

      $scope.heading = 'System Preferences';
    } else if (rSource === 'NAMESPACE') {
      path = '/namespaces/' + $state.params.nsadmin + '/preferences';
      parentPath = '/preferences';
      $scope.messages = 'Specify new or override system configurations that will be accessible in all applications & datasets within this namespace.';

      $scope.heading = 'Namespace Preferences: ' + $state.params.nsadmin;
    } else if (rSource === 'APPLICATION') {
      path = '/namespaces/' + $state.params.nsadmin + '/apps/' + $state.params.appId + '/preferences';
      parentPath = '/namespaces/' + $state.params.nsadmin + '/preferences?resolved=true';
      $scope.messages = 'Specify new or override namespace configurations that will be accessible in all programs within this application.';

      $scope.heading = 'Application Preferences: ' + $state.params.appId;
    }

    $scope.preferences = [];

    $scope.loadProperties = function () {
      if (rSource !== 'SYSTEM') {
        dataSrc
          .request({
            _cdapPath: parentPath
          })
          .then(function (res) {

            var arr = [];

            angular.forEach(res, function(v, k) {
              arr.push({
                key: k,
                value: v
              });
            });

            $scope.parentPreferences = arr;
          });
      }

      dataSrc
        .request({
          _cdapPath: path
        }).then(function (res) {
          var arr = [];
          angular.forEach(res, function(v, k) {
            arr.push({
              key: k,
              value: v
            });
          });


          $scope.preferences = arr;
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

      dataSrc
        .request({
          _cdapPath: path,
          method: 'PUT',
          body: obj
        })
        .then(function() {
          $alert({
            content: 'Your preferences have been successfully saved!',
            type: 'success'
          });
          $scope.loadProperties();
        });
    };

    $scope.deletePreferences = function() {
      dataSrc
        .request({
          _cdapPath: path,
          method: 'DELETE'
        })
        .then(function() {
          $scope.loadProperties();
          $alert({
            content: 'Your preferences have been deleted',
            type: 'success'
          });
        });
    };

  });
