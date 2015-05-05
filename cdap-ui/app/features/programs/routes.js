angular.module(PKG.name + '.feature.programs')
  .config(function($stateProvider, $urlRouterProvider, MYAUTH_ROLE) {
    $stateProvider
      .state('programs', {
        parent: 'apps.detail',
        url: '/programs',
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'development'
        },
        template: '<ui-view/>',
        abstract: true
      });
  });
