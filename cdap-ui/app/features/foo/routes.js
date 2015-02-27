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
      .state('test-edwin', {
        url: '/test/edwin',
        templateUrl: '/assets/features/foo/edwin.html',
        controller: function ($scope, $timeout) {
          $scope.data = {};
          $timeout(function() {
            $scope.data = {
              nodes: [
                'one',
                'two',
                'three',
                'four',
                'five',
                'whew',
                'err',
                'six',
                'seven'
              ],
              edges: [
                { sourceName: 'one', targetName: 'three' },
                { sourceName: 'two', targetName: 'three' },
                { sourceName: 'three', targetName: 'four' },
                { sourceName: 'three', targetName: 'five'},
                { sourceName: 'whew', targetName: 'seven'},
                { sourceName: 'four', targetName: 'seven' },
                { sourceName: 'six', targetName: 'seven'},
                { sourceName: 'err', targetName: 'seven'},
                { sourceName: 'five', targetName: 'six' },
                { sourceName: 'two', targetName: 'seven'},
                { sourceName: 'four', targetName: 'six'}
              ]
            };
          }, 3000);
        }
      })

      .state('test-settings', {
        url: '/test/settings',
        templateUrl: '/assets/features/foo/settings.html',
        controller: 'FooPlaygroundController'
      });

  });
