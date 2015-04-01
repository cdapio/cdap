angular.module(PKG.name + '.feature.admin')
  .controller('NamespacePreferencesController', function ($scope, $filter, MyDataSource, $alert, $state) {
    var dataSrc = new MyDataSource($scope);
    var filterFilter = $filter('filter');

    var path = '/namespaces/' + $state.params.nsadmin + '/preferences';

    $scope.preferences = [];

    $scope.loadProperties = function () {
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
    }

    $scope.removePreference = function(preference) {
      var match = filterFilter($scope.preferences, preference);
      if (match.length) {
        $scope.preferences.splice($scope.preferences.indexOf(match[0]), 1);
      }
    };

    $scope.save = function() {
      var obj = {};

      angular.forEach($scope.preferences, function(v) {
        obj[v.key] = v.value;
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