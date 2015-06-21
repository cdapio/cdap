angular.module(PKG.name+'.feature.login')
  .config(function ($stateProvider) {

    /**
     * State Configurations
     */
    $stateProvider

      .state('login', {
        url: '/login?next',
        templateUrl: '/assets/features/login/login.html',
        controller: 'LoginCtrl',
        onEnter: function(MY_CONFIG, myLoadingService, myAuth, $rootScope, MYAUTH_EVENT) {
          if(!MY_CONFIG.securityEnabled) {
            myLoadingService
              .showLoadingIcon()
              .then(function() {
                return myAuth.login({username:'admin'});
              })
              .then(function() {
                myLoadingService.hideLoadingIcon();
              });
          } else {
            if (myAuth.isAuthenticated()) {
              $rootScope.$broadcast(MYAUTH_EVENT.loginSuccess);
            }
          }
        }
      });
  })
  .run(function ($rootScope, $state, $alert, $location, MYAUTH_EVENT, myNamespace, $q, myHelpers) {

    $rootScope.$on(MYAUTH_EVENT.loginSuccess, function onLoginSuccess() {
      // General case: User logs in and we emit login success event.
      // In that case making the namespace list call is un-necessary - we know user session has just begun.
      // Another case: If we are in a nested child state and we hit refresh in the browser
      // We go to login, see if we have token - if we do then emit login success.
      // But that token might be expired and so we need to re-authenticate.
      var defer = $q.defer();
      myNamespace
        .getList()
        .then(
          function success() {
            defer.resolve();
            return defer.promise;
          },
          function error(err) {
            defer.reject(err);
            return defer.promise;
          })
        .then(
          function onValidToken() {
            var next = $state.is('login') && $state.params.next;
            if(next && next.path) {
              next = angular.fromJson(next);
              console.log('After login, will redirect to:', next);
              $location
              .path(next.path);
              // FIXME: This has un-certain behavior. Need to fix this
              // .search(next.search)
              // .replace();
            } else {
              $state.go('overview');
            }
          },
          function onInvalidToken(err) {
            if (myHelpers.objectQuery(err, 'data', 'auth_uri')) {
              $rootScope.$broadcast(MYAUTH_EVENT.sessionTimeout);
              $state.go('login');
            }
          }
        );
    });

  })
  .run(function ($rootScope, $state, $alert, MYAUTH_EVENT, MY_CONFIG, myAlert, myAuth) {

    $rootScope.$on(MYAUTH_EVENT.logoutSuccess, function () {
      $state.go('login');
    });

    $rootScope.$on(MYAUTH_EVENT.sessionTimeout, function() {
      $alert({
        type: 'danger',
        title: 'Session Timeout',
        message: 'Your current session has timed out. Please login again.'
      });
      myAuth.logout();
    })

    if(MY_CONFIG.securityEnabled) {
      angular.forEach([
          {
            event: MYAUTH_EVENT.loginFailed,
            eventType: 'danger',
            title: 'Login Failed',
            message: 'User Authentication failed! Please check username and password'
          },
          {
            event: MYAUTH_EVENT.notAuthenticated,
            eventType: 'danger',
            title: 'Authentication required',
            message: 'This page needs user to be authenticated. Please login to this page.'
          }
        ],
        function (v) {
          $rootScope.$on(v.event, function () {
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
