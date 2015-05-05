angular.module(PKG.name + '.feature.userprofile')
  .config(function($stateProvider, $urlRouterProvider, MYAUTH_ROLE) {
    $stateProvider
      .state('userprofile', {
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'development'
        },
        url: '/profile',
        templateUrl: '/assets/features/userprofile/templates/profile.html',
        controller: 'UserProfileController'
      });
  });
