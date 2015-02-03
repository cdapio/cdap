angular.module(PKG.name + '.feature.flows')
  .config(function($stateProvider, $urlRouterProvider, MYAUTH_ROLE) {
    $stateProvider
      .state('flows', {
        url: '/flows',
        abstract: true,
        parent: 'programs',
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'development'
        },
        template: '<ui-view/>'
      })
        .state('flows.list', {
          url: '/list',
          templateUrl: '/assets/features/flows/templates/list.html',
          controller: 'CdapflowsListController',
          ncyBreadcrumb: {
            parent: 'apps.detail.overview',
            label: 'Flows'
          }
        })
        .state('flows.detail', {
          url: '/:programId',
          templateUrl: '/assets/features/flows/templates/detail.html',
          ncyBreadcrumb: {
            parent: 'flows.list',
            label: '{{$state.params.programId | caskCapitalizeFilter}}'
          }
        })
          .state('flows.detail.tab', {
            url: '/:tab',
            ncyBreadcrumb: {
              skip: true
            }
          });
  });
