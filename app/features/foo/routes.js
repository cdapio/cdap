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
          mySettings.get('test').then(function (result){
            $scope.model = result;
          });
          $scope.doSave = function () {
            mySettings.set('test', $scope.model);
          };
        }
      });

  });
