/**
 * Login Ctrl
 */

angular.module(PKG.name+'.feature.login').controller('LoginCtrl',
function ($scope, myAuth, $alert, $state, cfpLoadingBar, $timeout, MYAUTH_EVENT, MY_CONFIG, caskFocusManager, myNamespace) {

  $scope.credentials = myAuth.remembered();

  $scope.submitting = false;

  $scope.doLogin = function (c) {
    $scope.submitting = true;
    cfpLoadingBar.start();

    myAuth.login(c)
      .finally(function(){
        // Get state params used in the previous state (if any)
        var params = stateParamsCache.getCache();
        var isParams = Object.keys(params).length;

        // If you are already on a namespace(apps/data view) then use it to navigate to the overview page.
        if ($state.params.namespaceId) {
          $state.go('ns.overview', {namespaceId: $state.params.namespaceId});
        } else if (isParams){

          // Transition from /login?next state to a state that you were previously in.
          if ($state.params.next && isParams) {
            $state.go(next, params);
          }

        } else {

          // Logging in for the firt time (no history of previous state or any selected namespaces)
          // So get the list and select the default option.
          myNamespace.getList()
            .then(function(list) {
              $state.go('ns.overview', {namespaceId: list[0].name});
            });

        }
        $alert({
          title:'Welcome!',
          content:'You\'re logged in!',
          type:'success'
        });
        $scope.submitting = false;
        cfpLoadingBar.complete();
      });
  };

  $scope.$on('$viewContentLoaded', function() {
    if(myAuth.isAuthenticated()) {
      $alert({
        content: 'You are already logged in!',
        type: 'warning'
      });
    }
    else {

      if(MY_CONFIG.securityEnabled) {
        focusLoginField();
      }
      else { // auto-login
        console.log(0);
        myAuth.login({username:'admin'});
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
