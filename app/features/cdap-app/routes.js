angular.module(PKG.name+'.feature.cdap-app')
  .config(function ($stateProvider, $urlRouterProvider) {

    /**
     * State Configurations
     */
    $stateProvider

      .state('cdap-app', {
        abstract: true,
        template: '<ui-view/>',
        url: '/app',
        parent: 'ns'
      })

      .state('cdap-app.list', {
        url: '/list',
        templateUrl: '/assets/features/cdap-app/templates/list.html',
        controller: 'ApplicationListController',
        ncyBreadcrumb: {
          label: 'Applications',
          parent: 'overview'
        }
      })

      .state('cdap-app.detail', {
        url: '/view/:appId',
        templateUrl: '/assets/features/cdap-app/templates/application.html',
        controller: 'ApplicationController',
        ncyBreadcrumb: {
          parent: 'cdap-app.list',
          label: '{{$state.params.appId}}'
        }
      })
        .state('cdap-app.detail.tab', {
          url: '/:tab',
          ncyBreadcrumb: {
            skip: true
          }
        });

  });
