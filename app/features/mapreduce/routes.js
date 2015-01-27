angular.module(PKG.name + '.feature.mapreduce')
  .config(function($stateProvider, $urlRouterProvider, MYAUTH_ROLE) {
    $stateProvider
      .state('mapreduce', {
        url: '',
        abstract: true,
        parent: 'programs',
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'development'
        },
        template: '<ui-view/>'
      })
      .state('mapreduce.detail', {
        url: '/:programId',
        templateUrl: '/assets/features/mapreduce/templates/detail.html',
        controller: 'CdapMapreduceDetailController',
        ncyBreadcrumb: {
          parent: 'programs.type',
          label: '{{$state.params.programId | caskCapitalizeFilter}}'
        }
      })
        .state('mapreduce.detail.tab', {
          url: '/:tab',
          ncyBreadcrumb: {
            skip: true
          }
        });
  });
