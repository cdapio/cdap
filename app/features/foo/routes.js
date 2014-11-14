angular.module(PKG.name+'.feature.foo')
  .config(function ($stateProvider) {


    /**
     * State Configurations
     */
    $stateProvider

      .state('foo', {
        url: '/foo',
        templateUrl: '/assets/features/foo/foo.html'
      })

        .state('bar', {
          parent: 'foo',
          url: '/bar'
        })

      ;


  });
