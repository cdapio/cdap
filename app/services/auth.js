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
   * private method to sync the user everywhere
   */
  var persist = angular.bind(this, function (u) {
    this.currentUser = u;
    $rootScope.currentUser = u;
    $localStorage.currentUser = u ? u.getStorageInfo() : null;
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
        persist(user);
        $localStorage.remember = credentials.remember && user.getStorageInfo();
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


module.factory('myAuthPromise', function myAuthPromiseFactory (MYAUTH_ROLE, $timeout, $q, $http) {
  return function myAuthPromise (credentials) {
    var deferred = $q.defer();

    $http({
      url: '/login',
      method: 'POST',
      data: credentials
    })
    .success(function (data, status, headers, config) {
      deferred.resolve(angular.extend(data, {
        username: credentials.username
      }));
    })
    .error(function (data, status, headers, config) {
      deferred.reject(data);
    });

    return deferred.promise;
  };
});


module.factory('MyAuthUser', function MyAuthUserFactory (MYAUTH_ROLE) {

  /**
   * Constructor for currentUser data
   * @param {object} user data
   */
  function User(data) {
    this.token = data.token;
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
    // All roles authorized.
    return true;
  };

  /**
   * Omits secure info (i.e. token) and gets object for storage.
   * @return {Object} storage info.
   */
  User.prototype.getStorageInfo = function () {
    return {
      username: this.username
    };
  };

  return User;
});
