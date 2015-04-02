/**
 * Login Ctrl
 */

angular.module(PKG.name+'.feature.login').controller('LoginCtrl',
function ($scope, myAuth, myAlert, $state, cfpLoadingBar, $timeout,
   MYAUTH_EVENT, MY_CONFIG, caskFocusManager) {

  $scope.credentials = myAuth.remembered();
  $scope.submitting = false;

  $scope.isAuthenticated = MY_CONFIG.securityEnabled;

  $scope.doLogin = function (c) {
    $scope.submitting = true;
    cfpLoadingBar.start();

    myAuth.login(c)
      .finally(function() {
        $scope.submitting = false;
        cfpLoadingBar.complete();
      });
  };

  $scope.$on('$viewContentLoaded', function() {
    if(myAuth.isAuthenticated()) {
      myAlert({
        content: 'You are already logged in!',
        type: 'warning'
      });
      $state.go('overview');
    }
    else {

      if(MY_CONFIG.securityEnabled) {
        focusLoginField();
      }

    }
  });

  $scope.$on(MYAUTH_EVENT.loginFailed, focusLoginField);

  /* ----------------------------------------------------------------------- */

  function focusLoginField() {
    $timeout(function() {
      caskFocusManager.select($scope.credentials.username ? 'password' : 'username');
    }, 10); // the addtl timeout is so this triggers AFTER any potential focus() on an $alert
  }

});
