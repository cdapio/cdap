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

      .state('bar', {
        parent: 'foo',
        url: '/bar'
      })

        .state('apps', {
          data: {
            authorizedRoles: MYAUTH_ROLE.all
          },
          parent: 'foo',
          url: '/apps'
        })

        .state('admin', {
          data: {
            authorizedRoles: MYAUTH_ROLE.admin
          },
          parent: 'foo',
          url: '/admin'
        })

      ;


  });
