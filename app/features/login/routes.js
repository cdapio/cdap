angular.module(PKG.name+'.feature.login')
  .config(function ($stateProvider, $urlRouterProvider) {

    /**
     * Redirects and Otherwise
     */
    $urlRouterProvider
      .when('/signin', '/login');


    /**
     * State Configurations
     */
    $stateProvider

      .state('login', {
        url: '/login',
        templateUrl: '/assets/features/login/login.html',
        controller: 'LoginCtrl'
      })

      ;


  })
  .run(function ($rootScope, $state, $alert, myAuth, MYAUTH_EVENT, MYAUTH_ROLE) {

    $rootScope.$on(MYAUTH_EVENT.loginSuccess, function () {
      $alert({title:'Welcome!', content:'You\'re logged in!', type:'success'});
      $state.go(myAuth.currentUser.hasRole(MYAUTH_ROLE.admin) ? 'home' : 'login');
    });

    $rootScope.$on(MYAUTH_EVENT.logoutSuccess, function () {
      $alert({title:'Bye!', content:'You are now logged out.', type:'info'});
      $state.go('login');
    });

    $rootScope.$on(MYAUTH_EVENT.notAuthorized, function () {
      $alert({title:'Authentication error!', content:'You are not allowed to access the requested page.', type:'warning'});
      $state.go('login');
    });

    angular.forEach([
        MYAUTH_EVENT.loginFailed,
        MYAUTH_EVENT.sessionTimeout,
        MYAUTH_EVENT.notAuthenticated
      ],
      function (v) {
        $rootScope.$on(v, function (event) {
          $alert({title:event.name, type:'danger'});
          if(!$state.is('login')) {
            $state.go('login');
          }
        });
      }
    );

  });
