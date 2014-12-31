angular.module(PKG.name+'.feature.login')
  .config(function ($stateProvider, $urlRouterProvider) {

    $urlRouterProvider.when('/', '/login');

    /**
     * State Configurations
     */
    $stateProvider

      .state('login', {
        url: '/login?next',
        templateUrl: '/assets/features/login/login.html',
        controller: 'LoginCtrl'
      })

      ;


  })
  .run(function ($rootScope, $state, $alert, $location, MYAUTH_EVENT) {

    $rootScope.$on(MYAUTH_EVENT.loginSuccess, function () {
      var next = $state.is('login') && $state.params.next;
      var currentUser = event.currentScope.currentUser.username;
      if (currentUser && next) {
          $rootScope.$applyAsync(function() {
            $rootScope.$location.path(next);
            $rootScope.$location.url($rootScope.$location.path());
          });
      } else {
        $state.go('ns.overview');
      }
    });

  })
  .run(function ($rootScope, $state, $alert, MYAUTH_EVENT, MY_CONFIG) {

    $rootScope.$on(MYAUTH_EVENT.logoutSuccess, function () {
      $alert({title:'Bye!', content:'You are now logged out.', type:'info'});
      $state.go('login');
    });

    $rootScope.$on(MYAUTH_EVENT.notAuthorized, function () {
      $alert({title:'Authentication error!', content:'You are not allowed to access the requested page.', type:'warning'});
    });

    if(MY_CONFIG.securityEnabled) {
      angular.forEach([
          MYAUTH_EVENT.loginFailed,
          MYAUTH_EVENT.sessionTimeout,
          MYAUTH_EVENT.notAuthenticated
        ],
        function (v) {
          $rootScope.$on(v, function (event) {
            $alert({title:event.name, type:'danger'});
          });
        }
      );
    }

  });
