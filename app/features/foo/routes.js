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
        controller: function ($scope) {
          $scope.data = {
            nodes: [
              { id: 'one', value: { label: 'one' } },
              { id: 'two', value: { label: 'two' } },
              { id: 'three', value: { label: 'three' } },
              { id: 'four', value: { label: 'four' } },
              { id: 'five', value: { label: 'five' } },
              { id: 'whew', value: { label: 'whew' } },
              { id: 'err', value: { label: 'err' } },
              { id: 'six', value: { label: 'six' } },
              { id: 'seven', value: { label: 'seven' } }
            ],
            links: [
              { u: 'one', v: 'three' },
              { u: 'two', v: 'three' },
              { u: 'three', v: 'four' },
              { u: 'three', v: 'five'},
              { u: 'whew', v: 'seven'},
              { u: 'four', v: 'seven' },
              { u: 'six', v: 'seven'},
              { u: 'err', v: 'seven'},
              { u: 'five', v: 'six' },
              { u: 'two', v: 'seven'},
              { u: 'four', v: 'six'}
            ]
          };
        }
      })

      .state('test-settings', {
        url: '/test/settings',
        templateUrl: '/assets/features/foo/settings.html',
        controller: 'FooPlaygroundController'
      });

  });
