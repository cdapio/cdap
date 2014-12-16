angular.module(PKG.name+'.feature.dashboard')
  .config(function ($stateProvider, $urlRouterProvider) {

    /**
     * State Configurations
     */
    $stateProvider

      .state('dashboard', {
        url: '/dashboard',
        templateUrl: '/assets/features/dashboard/templates/dashboard.html',
        controller: 'DashboardCtrl'
      })

        .state('dashboard.tab', {
          url: '?tab'
        })
      ;


  });
