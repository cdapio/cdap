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
        templateUrl: '/assets/features/data/datasets/templates/list.html',
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
          templateUrl: '/assets/features/data/datasets/templates/detail.html',
          controller: 'CdapDatasetDetailController',
          ncyBreadcrumb: {
            parent: 'datasets.list',
            label: '{{$state.params.datasetsId}}'
          }
        })
          .state('datasets.detail.overview.tab', {
            url: '/:tab',
            ncyBreadcrumb: {
              skip: true
            }
          });
  });
