angular.module(PKG.name+'.feature.cdap-app')
  .config(function ($stateProvider, $urlRouterProvider, MYAUTH_ROLE) {

    /**
     * State Configurations
     */
    $stateProvider

      .state('cdap-app', {
        abstract: true,
        template: '<ui-view/>',
        url: '/apps',
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'development'
        },
        parent: 'ns'
      })

      .state('cdap-app.list', {
        url: '',
        templateUrl: '/assets/features/cdap-app/templates/list.html',
        controller: 'CdapAppListController',
        ncyBreadcrumb: {
          label: 'Applications',
          parent: 'overview'
        }
      })

      .state('cdap-app.detail', {
        url: '/:appId',
        abstract: true,
        template: '<ui-view/>'
      })
        .state('app-overview', {
          url: '/overview',
          parent: 'cdap-app.detail',
          templateUrl: '/assets/features/cdap-app/templates/detail.html',
          controller: 'CdapAppDetailController',
          ncyBreadcrumb: {
            parent: 'cdap-app.list',
            label: '{{$state.params.appId}}'
          }
        })
          .state('app-overview.tab', {
            url: '/:tab',
            ncyBreadcrumb: {
              parent: 'cdap-app.detail',
              label: '{{$state.params.tabId}}'
            }
          });

  });
