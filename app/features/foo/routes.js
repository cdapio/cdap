angular.module(PKG.name+'.feature.foo')
  .config(function ($stateProvider, MYAUTH_ROLE) {


    /**
     * State Configurations
     */
    $stateProvider

      .state('foo', {
        url: '/foo',
        templateUrl: '/assets/features/foo/foo.html'
      })

      .state('test-settings', {
        url: '/test/settings',
        templateUrl: '/assets/features/foo/settings.html',
        controller: function ($scope, mySettings) {
          $scope.model = mySettings.get('test');
          $scope.doSave = function () {
            mySettings.set('test', $scope.model);
          };
        }
      });

  });
