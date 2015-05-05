angular.module(PKG.name+'.feature.overview')
  .config(function ($stateProvider, $urlRouterProvider, MYAUTH_ROLE) {

    /**
     * State Configurations
     */
    $stateProvider

      .state('overview', {
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'development'
        },
        parent: 'ns',
        url: '',
        templateUrl: '/assets/features/overview/overview.html',
        controller: 'OverviewCtrl',
        ncyBreadcrumb: {
          label: 'Home'
        }
      })


      ;


  });
