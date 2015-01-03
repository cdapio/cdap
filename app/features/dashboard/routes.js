angular.module(PKG.name+'.feature.dashboard')
  .config(function ($stateProvider, $urlRouterProvider, MYAUTH_ROLE) {

    $urlRouterProvider
      .when('/dashboard', '/dashboard/0');


    /**
     * State Configurations
     */
    $stateProvider

      .state('dashboard', {
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'operations'
        },
        parent: 'ns',
        url: '/dashboard/:tab',
        templateUrl: '/assets/features/dashboard/main.html',
        controller: 'DashboardCtrl'
      })

        .state('dashboard.addwdgt', {
          // url: '/widget/add',
          onEnter: function ($state, $modal) {
            $modal({
              template: '/assets/features/dashboard/partials/addwdgt.html'
            }).$promise.then(function () {
              $state.go('^', $state.params);
            });
          }
        })

      ;


  });
