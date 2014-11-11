var module = angular.module(PKG.name+'.services');

/*
  inspired by https://medium.com/opinionated-angularjs/
    techniques-for-authentication-in-angularjs-applications-7bbf0346acec
 */

module.constant('MYAUTH_EVENT', {
  loginSuccess: 'myauth-login-success',
  loginFailed: 'myauth-login-failed',
  logoutSuccess: 'myauth-logout-success',
  sessionTimeout: 'myauth-session-timeout',
  notAuthenticated: 'myauth-not-authenticated',
  notAuthorized: 'myauth-not-authorized'
});


module.constant('MYAUTH_ROLE', {
  all: '*',
  superadmin: 'superadmin',
  admin: 'admin'
});




module.run(function ($rootScope, myAuth, MYAUTH_EVENT, MYAUTH_ROLE) {
  $rootScope.currentUser = myAuth.currentUser;

  $rootScope.$on('$stateChangeStart', function (event, next) {
    var authorizedRoles = next.data && next.data.authorizedRoles;
    if (!authorizedRoles) { return; } // no role required, anyone can access

    var user = myAuth.currentUser;
    if (user) { // user is logged in
      if (authorizedRoles === MYAUTH_ROLE.all) { return; } // any logged-in user is welcome
      if (user.hasRole(authorizedRoles)) { return; } // user is legit
    }
    // in all other cases, prevent going to this state
    event.preventDefault();
    $rootScope.$broadcast(user ? MYAUTH_EVENT.notAuthorized : MYAUTH_EVENT.notAuthenticated);
  });

});







module.service('myAuth', function myAuthService (MYAUTH_EVENT, MyAuthUser, myAuthPromise, $rootScope, $localStorage) {

  /**
   * currentUser is initially revived with data in storage (or null)
   */
  this.currentUser = MyAuthUser.revive($localStorage.currentUser);

  /**
   * private method to sync the user everywhere
   */
  var persist = angular.bind(this, function (u) {
    this.currentUser = u;
    $localStorage.currentUser = u;
    $rootScope.currentUser = u;
  });

  /**
   * remembered
   * @return {object} credentials
   */
  this.remembered = function () {
    var r = $localStorage.remember;
    return angular.extend({
      remember: !!r
    }, r || {});
  };

  /**
   * login
   * @param  {object} credentials
   * @return {promise} resolved on sucessful login
   */
  this.login = function (credentials) {
    return myAuthPromise(credentials).then(
      function(data) {
        var user = new MyAuthUser(data);
        persist( user );
        $localStorage.remember = credentials.remember && user;
        $rootScope.$broadcast(MYAUTH_EVENT.loginSuccess);
      },
      function() {
        $rootScope.$broadcast(MYAUTH_EVENT.loginFailed);
      }
    );
  };

  /**
   * logout
   */
  this.logout = function () {
    persist(null);
    $rootScope.$broadcast(MYAUTH_EVENT.logoutSuccess);
  };

  /**
   * is there someone here?
   * @return {Boolean}
   */
  this.isAuthenticated = function () {
    return !!this.currentUser;
  };

});





module.factory('myAuthPromise', function myAuthPromiseFactory (MYAUTH_ROLE, $timeout, $q) {
  return function myAuthPromise (c) {
    var deferred = $q.defer();



    /*
      fake login / replacement pending backend support
     */
    $timeout(function(){
      if (!c.password || !c.tenant || !c.username) {
        deferred.reject();
      }
      else {
        var a = MYAUTH_ROLE.admin;
        if (c.username===a && c.password!==a) {
          deferred.reject();
        }
        else {
          delete c.password;
          deferred.resolve(c);
        }
      }
    }, 1500);



    return deferred.promise;
  };
});






module.factory('MyAuthUser', function MyAuthUserFactory (MYAUTH_ROLE) {

  /**
   * Constructor for currentUser data
   * @param {object} user data
   */
  function User(data) {
    this.username = data.username;
    this.tenant = data.tenant;

    // wholly insecure while we wait for real auth
    if (data.username===MYAUTH_ROLE.admin) {
      if(data.tenant===MYAUTH_ROLE.superadmin) {
        this.role = MYAUTH_ROLE.superadmin;
      }
      else if(data.tenant) {
        this.role = MYAUTH_ROLE.admin;
      }
    }
  }

  /**
   * attempts to make a User from data
   * @param  {Object} stored data
   * @return {User|null}
   */
  User.revive = function(data) {
    return angular.isObject(data) ? new User(data) : null;
  };

  /**
   * do i haz one of given roles?
   * @param  {String|Array} authorizedRoles
   * @return {Boolean}
   */
  User.prototype.hasRole = function(authorizedRoles) {
    if(this.role === MYAUTH_ROLE.superadmin) {
      return true;
    }
    if (!angular.isArray(authorizedRoles)) {
      authorizedRoles = [authorizedRoles];
    }
    return authorizedRoles.indexOf(this.role) !== -1;
  };

  return User;
});
