angular.module(PKG.name+'.feature.home')
  .config(function ($stateProvider, $urlRouterProvider, $urlMatcherFactoryProvider) {

    /**
     * Ignores trailing slash
     */
    $urlMatcherFactoryProvider.strictMode(false);

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
        onEnter: function(MY_CONFIG, myAuth, $state, myLoadingService, $rootScope, MYAUTH_EVENT) {
          if (!MY_CONFIG.securityEnabled) {
            // Skip even the login view. Don't show login if security is disabled.
            myAuth.login({username:'admin'})
              .then(function() {
                myLoadingService.showLoadingIcon();
                $state.go('overview');
              });
          } else {
            if (myAuth.isAuthenticated()) {
              $rootScope.$broadcast(MYAUTH_EVENT.loginSuccess);
            } else {
              $state.go('login');
            }
          }
        }
      })

      .state('ns', {
        url: '/ns/:namespace',
        abstract: true,
        template: '<ui-view/>',
        resolve: {
          rNsList: function (myNamespace) {
            return myNamespace.getList();
          }
        },
        controller: 'HomeController'
      })

      .state('404', {
        templateUrl: '/assets/features/home/404.html'
      })

      ;


  });
