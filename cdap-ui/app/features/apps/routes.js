angular.module(PKG.name+'.feature.apps')
  .config(function ($stateProvider, $urlRouterProvider, MYAUTH_ROLE) {

    /**
     * State Configurations
     */
    $stateProvider

      .state('apps', {
        abstract: true,
        template: '<ui-view/>',
        url: '/apps',
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'development'
        },
        parent: 'ns'
      })

      .state('apps.list', {
        url: '',
        templateUrl: '/assets/features/apps/templates/list.html',
        controller: 'AppListController',
        controllerAs: 'ListController',
        ncyBreadcrumb: {
          label: 'Applications',
          parent: 'overview'
        }
      })

      .state('apps.detail', {
        url: '/:appId',
        abstract: true,
        template: '<ui-view/>'
      })
        .state('apps.detail.overview', {
          url: '/overview',
          templateUrl: '/assets/features/apps/templates/detail.html',
          ncyBreadcrumb: {
            skip: true
          }
        })
          .state('apps.detail.overview.status', {
            url: '/status',
            ncyBreadcrumb: {
              parent: 'apps.list',
              label: '{{$state.params.appId}}'
            },
            controller: 'AppDetailStatusController',
            controllerAs: 'StatusController',
            templateUrl: '/assets/features/apps/templates/tabs/status.html'
          })

          .state('apps.detail.overview.datasets', {
            url: '/datasets',
            ncyBreadcrumb: {
              label: 'Datasets',
              parent: 'apps.detail.overview.status'
            },
            templateUrl: '/assets/features/apps/templates/tabs/datasets.html'
          });

  });
