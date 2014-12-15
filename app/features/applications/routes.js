angular.module(PKG.name+'.feature.applications')
  .config(function ($stateProvider, $urlRouterProvider) {

    /**
     * State Configurations
     */
    $stateProvider

      .state('apps', {
        url: '/ns/:namespace',
        templateUrl: '/assets/features/applications/templates/apps.html',
        resolve: {
          namespace: function() {
            return 'ns1';
          }
        }
      })

      .state('apps.list', {
        url: '/apps',
        templateUrl: '/assets/features/applications/templates/list.html',
        controller: 'ApplicationListController',
        ncyBreadcrumb: {
          label: 'Applications',
          parent: 'home'
        }
      })

      .state('apps.application', {
        url: '/apps/:appId',
        templateUrl: '/assets/features/applications/templates/application.html',
        controller: 'ApplicationController',
        ncyBreadcrumb: {
          parent: 'apps.list',
          label: '{{$state.params.appId}}'
        }
      })
        .state('apps.application.tab', {
          url: '/:tab',
          ncyBreadcrumb: {
            skip: true
          }
        });

  });
