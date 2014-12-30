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

      .state('ns', {
        data: {
          authorizedRoles: MYAUTH_ROLE.all
        },
        url: '/ns/:namespaceId',
        abstract: true,
        resolve: {
          namespaceList: function(myNamespace) {
            return myNamespace.getList();
          }
        },
        templateUrl: '/assets/features/home/home.html'
      })
        .state('ns.overview', {
          url: '',
          templateUrl: '/assets/features/home/overview.html',
          controller: 'HomeCtrl'
        })

      .state('404', {
        templateUrl: '/assets/features/home/404.html'
      })

      ;


  });
