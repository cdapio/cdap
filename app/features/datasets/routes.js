angular.module(PKG.name + '.feature.datasets')
  .config(function($stateProvider, $urlRouterProvider, MYAUTH_ROLE) {
    $stateProvider
      .state('datasets', {
        abstract: true,
        template: '<ui-view/>',
        url: '/datasets',
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'development'
        },
        parent: 'ns'
      })

      .state('datasets.list', {
        url: '',
        templateUrl: '/assets/features/datasets/templates/list.html',
        controller: 'CdapDatasetsListController',
        ncyBreadcrumb: {
          label: 'Datasets',
          parent: 'overview'
        }
      })
  });
