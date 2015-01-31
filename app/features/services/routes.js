angular.module(PKG.name + '.feature.services')
  .config(function($stateProvider, $urlRouterProvider, MYAUTH_ROLE) {
    $stateProvider
      .state('services', {
        url: '',
        abstract: true,
        parent: 'programs',
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'development'
        },
        template: '<ui-view/>'
      })
      .state('services.detail', {
        url: '/:programId',
        templateUrl: '/assets/features/services/templates/detail.html',
        ncyBreadcrumb: {
          parent: 'programs.type',
          label: '{{$state.params.programId | caskCapitalizeFilter}}'
        }
      })
        .state('service.detail.tab', {
          url: '/:tab',
          ncyBreadcrumb: {
            skip: true
          }
        });
  });
