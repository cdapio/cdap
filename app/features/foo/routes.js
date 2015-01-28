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

      .state('test-prefs', {
        url: '/test/prefs',
        templateUrl: '/assets/features/foo/prefs.html',
        controller: function ($scope, mySettings) {
          // window.mySettings = mySettings;
          $scope.model = mySettings.get('test');
        }
      });

  });
