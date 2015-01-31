angular.module(PKG.name + '.feature.mapreduce')
  .config(function($stateProvider, $urlRouterProvider, MYAUTH_ROLE) {
    $stateProvider
      .state('mapreduce', {
        url: '/mapreduce',
        abstract: true,
        parent: 'programs',
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'development'
        },
        template: '<ui-view/>'
      })
      .state('mapreduce.list', {
        url: '/list',
        templateUrl: '/assets/features/mapreduce/templates/list.html',
        controller: 'CdapMapreduceListController',
        ncyBreadcrumb: {
          parent: 'apps.detail.overview',
          label: 'Mapreduce'
        }
      })
      .state('mapreduce.detail', {
        url: '/:programId',
        templateUrl: '/assets/features/mapreduce/templates/detail.html',
        ncyBreadcrumb: {
          parent: 'mapreduce.list',
          label: '{{$state.params.programId | caskCapitalizeFilter}}'
        }
      })
        .state('mapreduce.detail.tab', {
          url: '/:tab',
          ncyBreadcrumb: {
            skip: true
          }
        });
  });
