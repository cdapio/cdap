angular.module(PKG.name+'.feature.home')
  .config(function ($stateProvider, $urlRouterProvider, MYAUTH_ROLE) {

    /**
     * Redirects and Otherwise
     */
    $urlRouterProvider
      .otherwise(function($injector, $location){
        $injector.get('$state').go($location.path() ? '404' : 'home');
      });


    /**
     * State Configurations
     */
    $stateProvider

      .state('home', {
        url: '/',
        templateUrl: '/assets/features/home/home.html',
        controller: 'HomeCtrl',
        ncyBreadcrumb: {
          label: 'Home'
        }
      })

      .state('ns', {
        url: '/ns/:namespace',
        abstract: true,
        template: '<ui-view/>'
      })

      .state('ns-picker', {
        data: {
          authorizedRoles: MYAUTH_ROLE.all
        },
        controller: function(myNamespace, $state) {
          myNamespace.getList()
            .then(function(list) {
              $state.go('overview', {
                namespace: list[0].displayName
              });
            });
        }
      })

      .state('404', {
        templateUrl: '/assets/features/home/404.html'
      })

      ;


  });
