/**
 * Login Ctrl
 */

angular.module(PKG.name+'.feature.login').controller('LoginCtrl',
function ($scope, myAuth, myAlert, $state, cfpLoadingBar, $timeout,
   MYAUTH_EVENT, MY_CONFIG, caskFocusManager, myLoadingService, myAuth) {

  $scope.credentials = myAuth.remembered();
  $scope.submitting = false;

  $scope.isAuthenticated = MY_CONFIG.securityEnabled || myAuth.isAuthenticated();

  $scope.doLogin = function (c) {
    $scope.submitting = true;
    myLoadingService.showLoadingIcon();
    cfpLoadingBar.start();

    myAuth.login(c)
      .finally(function() {
        $scope.submitting = false;
        myLoadingService.hideLoadingIcon();
        cfpLoadingBar.complete();
      });
  };

  $scope.$on('$viewContentLoaded', function() {
    if(MY_CONFIG.securityEnabled) {
      focusLoginField();
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
