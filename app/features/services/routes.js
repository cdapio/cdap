angular.module(PKG.name + '.feature.services')
  .config(function($stateProvider, $urlRouterProvider, MYAUTH_ROLE) {
    $stateProvider
      .state('services', {
        url: '/services',
        abstract: true,
        parent: 'programs',
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'development'
        },
        template: '<ui-view/>'
      })
      .state('services.list', {
        url: '/list',
        templateUrl: '/assets/features/services/templates/list.html',
        controller: 'CdapServicesListController',
        ncyBreadcrumb: {
          parent: 'apps.detail.overview',
          label: 'Services'
        }
      })
      .state('services.detail', {
        url: '/:programId',
        templateUrl: '/assets/features/services/templates/detail.html',
        ncyBreadcrumb: {
          parent: 'services.list',
          label: '{{$state.params.programId | caskCapitalizeFilter}}'
        }
      })
        .state('services.detail.tab', {
          url: '/:tab',
          ncyBreadcrumb: {
            skip: true
          }
        });
  });
