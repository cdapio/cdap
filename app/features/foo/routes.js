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

        .state('apps', {
          parent: 'foo',
          url: '/apps'
        })

        .state('admin', {
          parent: 'foo',
          url: '/admin'
        })

      ;


  });
