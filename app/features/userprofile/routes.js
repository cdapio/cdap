angular.module(PKG.name + '.feature.userprofile')
  .config(function($stateProvider, $urlRouterProvider, MYAUTH_ROLE) {
    $stateProvider
      .state('userProfile', {
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'development'
        },
        parent: 'ns',
        url: '/profile/:username',
        templateUrl: '/assets/features/userprofile/templates/profile.html',
        controller: 'UserProfileController'
      });
  });
