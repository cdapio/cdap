angular.module(PKG.name+'.feature.foo')
  .config(function ($stateProvider, MYAUTH_ROLE) {


    /**
     * State Configurations
     */
    $stateProvider

      .state('foo', {
        data: {
          authorizedRoles: MYAUTH_ROLE.admin  
        },
        url: '/foo',
        templateUrl: '/assets/features/foo/foo.html'
      })

      .state('bar', {
        data: {
          authorizedRoles: MYAUTH_ROLE.admin  
        },
        parent: 'foo',
        url: '/bar'
      })

        // .state('apps', {
        //   parent: 'foo',
        //   url: '/apps'
        // })

        .state('admin', {
          parent: 'foo',
          url: '/admin'
        })

      ;


  });
