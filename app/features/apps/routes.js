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
        controller: 'CdapAppListController',
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
        .state('app-overview', {
          url: '/overview',
          parent: 'apps.detail',
          templateUrl: '/assets/features/apps/templates/detail.html',
          controller: 'CdapAppDetailController',
          ncyBreadcrumb: {
            parent: 'apps.list',
            label: '{{$state.params.appId}}'
          }
        })
          .state('app-overview.tab', {
            url: '/:tab',
            ncyBreadcrumb: {
              parent: 'apps.detail',
              label: '{{$state.params.tabId}}'
            }
          });

  });
