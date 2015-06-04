angular.module(PKG.name+'.commons')

  .controller('ProgramPreferencesController', function($scope, $state, MyDataSource) {
    var dataSrc = new MyDataSource($scope);

    var parentPath = '/namespaces/' + $state.params.namespace
      + '/apps/' + $state.params.appId + '/preferences?resolved=true';

    var path = '/namespaces/' + $state.params.namespace
      + '/apps/' + $state.params.appId + '/'
      + $scope.type + '/' + $state.params.programId
      + '/preferences';

    $scope.heading = $state.params.programId + ' Preferences';

    $scope.preferences = [];

    $scope.loadProperties = function () {
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

          $scope.systemPreferences = arr;
        });

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

      $scope.preferences.splice($scope.preferences.indexOf(preference), 1);
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
          $scope.loadProperties();
          $scope.$close(obj);
        });
    };

  })

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
