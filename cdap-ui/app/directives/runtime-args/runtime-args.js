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
