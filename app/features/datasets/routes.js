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
          parent: 'data.list'
        }
      })

      .state('datasets.detail', {
        url: '/:datasetId',
        abstract: true,
        template: '<ui-view/>'
      })
        .state('datasets.detail.overview', {
          url: '/overview',
          parent: 'datasets.detail',
          templateUrl: '/assets/features/datasets/templates/detail.html',
          ncyBreadcrumb: {
            parent: 'data.list',
            label: '{{$state.params.datasetId | caskCapitalizeFilter}}'
          }
        })
          .state('datasets.detail.overview.tab', {
            url: '/:tab',
            ncyBreadcrumb: {
              skip: true
            }
          });
  });
