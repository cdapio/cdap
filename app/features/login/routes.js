angular.module(PKG.name+'.feature.login')
  .config(function ($stateProvider, $urlRouterProvider) {

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
      if(next) {
        next = angular.fromJson(next);
        console.log('After login, will redirect to:', next);
        $rootScope.$applyAsync(function() {
          $location
            .path(next.path)
            .search(next.search)
            .replace();
        });
      }
      else {
        $state.go('overview');
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
          {
            event: MYAUTH_EVENT.loginFailed,
            eventType: 'danger',
            title: 'Login Failed',
            message: 'User Authentication failed! Please check username and password'
          },
          {
            event: MYAUTH_EVENT.sessionTimeout,
            eventType: 'danger',
            title: 'Session Timeout',
            message: 'Your current session has timed out. Please login again.'
          },
          {
            event: MYAUTH_EVENT.notAuthenticated,
            eventType: 'danger',
            title: 'Authentication required',
            message: 'This page needs user to be authenticated. Please login to this page.'
          },
          {
            event: MYAUTH_EVENT.loginSuccess,
            eventType: 'success',
            title: 'Login Success!',
            message: 'You have been authenticated!'
          }
        ],
        function (v) {
          $rootScope.$on(v.event, function (event) {
            $alert({
              title: v.title,
              content: v.message,
              type: v.eventType
            });
          });
        }
      );
    }

  });
