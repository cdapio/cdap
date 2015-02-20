angular.module(PKG.name + '.feature.userprofile')
  .controller('UserProfileController', function($scope, $http, $alert, amMoment, $state) {
    $scope.alreadyLoggedIn = false;
    $scope.credentials = {
      username: $state.params.username,
      password: ''
    };

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
          var expirationTime = amMoment.preprocessDate(Date.now() + (res.expires_in * 1000));
          $scope.token = res.access_token;
          $scope.expirationTime = expirationTime.fromNow();
          $scope.alreadyLoggedIn = true;
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
