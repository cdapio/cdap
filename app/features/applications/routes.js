angular.module(PKG.name+'.feature.applications')
  .config(function ($stateProvider, $urlRouterProvider) {

    /**
     * State Configurations
     */
    $stateProvider

      .state('ns.apps', {
        abstract: true,
        templateUrl: '/assets/features/applications/templates/apps.html'
      })

      .state('ns.apps.list', {
        url: '/apps',
        templateUrl: '/assets/features/applications/templates/list.html',
        controller: 'ApplicationListController',
        ncyBreadcrumb: {
          label: 'Applications',
          parent: 'ns.overview'
        }
      })

      .state('ns.apps.application', {
        url: '/apps/:appId',
        templateUrl: '/assets/features/applications/templates/application.html',
        controller: 'ApplicationController',
        ncyBreadcrumb: {
          parent: 'ns.apps.list',
          label: '{{$state.params.appId}}'
        }
      })
        .state('ns.apps.application.tab', {
          url: '/:tab',
          ncyBreadcrumb: {
            skip: true
          }
        });

  });
