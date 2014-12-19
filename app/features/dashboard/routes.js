angular.module(PKG.name+'.feature.dashboard')
  .config(function ($stateProvider, $urlRouterProvider) {

    $urlRouterProvider
      .when('/dashboard', '/dashboard/0');


    /**
     * State Configurations
     */
    $stateProvider

      .state('dashboard', {
        url: '/dashboard/:tab',
        templateUrl: '/assets/features/dashboard/main.html',
        controller: 'DashboardCtrl'
      })

        .state('dashboard.addwdgt', {
          // url: '/widget/add',
          onEnter: function ($state, $modal) {
            var m = $modal({
              template: '/assets/features/dashboard/partials/addwdgt.html'
            });
            m.$promise.then(function () {
              $state.go('^', $state.params);
            });
          }
        })

      ;


  });
