angular.module(PKG.name + '.feature.data')
  .config(function($stateProvider, $urlRouterProvider, MYAUTH_ROLE) {
    $stateProvider
      .state('data', {
        abstract: true,
        template: '<ui-view/>',
        url: '/data',
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'development'
        },
        parent: 'ns'
      })

      .state('data.list', {
        url: '',
        templateUrl: '/assets/features/data/list.html',
        controller: 'CdapDataListController',
        ncyBreadcrumb: {
          label: 'Data',
          parent: 'overview'
        }
      });
  });
