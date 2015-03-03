angular.module(PKG.name + '.feature.userprofile')
  .controller('UserProfileController', function($scope, $http, $alert, myAuth) {
    $scope.reAuthenticated = false;
    $scope.credentials = {
      username: myAuth.currentUser.username
    };
    $scope.readonlyUsername = true;

    $scope.doLogin = function(credentials) {
      $http({
        method: 'POST',
        url: '/accessToken',
        data: {
          username: credentials.username,
          password: credentials.password
        }
      })
        .success(function(res) {
          $scope.token = res.access_token;
          $scope.expirationTime = Date.now() + (res.expires_in * 1000);
          $scope.reAuthenticated = true;
        })
        .error(function(res) {
          $alert({
            title: 'User Authentication Error!',
            content: 'Either Username or Password is wrong. Please try again',
            type: 'danger'
          });
        });
    };

  });
