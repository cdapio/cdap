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

      .state('admin', {
        data: {
          authorizedRoles: MYAUTH_ROLE.admin
        },
        parent: 'foo',
        url: '/admin'
      })

      ;


  });
