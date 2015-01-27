angular.module(PKG.name + '.feature.programs')
  .config(function($stateProvider, $urlRouterProvider, MYAUTH_ROLE) {
    $stateProvider
      .state('programs', {
        parent: 'apps.detail',
        url: '/programs/:programType',
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'development'
        },
        template: '<ui-view/>',
        abstract: true
      })
        .state('programs.type', {
          url: '',
          templateUrl: '/assets/features/programs/templates/list.html',
          controller: 'ProgramsListController',
          ncyBreadcrumb: {
            label: '{{$state.params.programType | caskCapitalizeFilter}}',
            parent: 'apps.detail.overview'
          }
        });
  });
