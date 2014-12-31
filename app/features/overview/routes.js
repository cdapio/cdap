angular.module(PKG.name+'.feature.overview')
  .config(function ($stateProvider, $urlRouterProvider, MYAUTH_ROLE) {

    /**
     * State Configurations
     */
    $stateProvider

      .state('overview', {
        data: {
          authorizedRoles: MYAUTH_ROLE.all
        },
        // parent: 'ns',
        templateUrl: '/assets/features/overview/overview.html',
        controller: 'OverviewCtrl',
        ncyBreadcrumb: {
          label: 'Home'
        }
      })


      ;


  });
