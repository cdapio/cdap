angular.module(PKG.name+'.commons')

  .controller('ProgramPreferencesController', function($scope, $state, MyDataSource, $filter) {
    var dataSrc = new MyDataSource($scope);
    var filterFilter = $filter('filter');

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
          $scope.loadProperties();
          $scope.modalClose();
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

    function close () {
      modalInstance.close();
    }

    this.show = function(type) {

      var scope = $rootScope.$new();
      scope.type = type;
      scope.modalClose = close;

      modalInstance = $bootstrapModal.open({
        template: '<my-program-preferences></my-program-preferences>',
        size: 'lg',
        scope: scope
      });
      return modalInstance;
    };

  });