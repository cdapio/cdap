angular.module(PKG.name + '.feature.streams')
  .config(function($stateProvider, $urlRouterProvider, MYAUTH_ROLE) {
    $stateProvider
      .state('streams', {
        abstract: true,
        template: '<ui-view/>',
        url: '/streams',
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'development'
        },
        parent: 'ns'
      })

      .state('streams.list', {
        url: '',
        templateUrl: '/assets/features/streams/templates/list.html',
        controller: 'CdapStreamsListController',
        ncyBreadcrumb: {
          label: 'Streams',
          parent: 'data.list'
        }
      })

      .state('streams.detail', {
        url: '/:streamId',
        abstract: true,
        template: '<ui-view/>'
      })
        .state('streams.detail.overview', {
          url: '/overview',
          parent: 'streams.detail',
          templateUrl: '/assets/features/streams/templates/detail.html',
          controller: 'CdapStreamDetailController',
          ncyBreadcrumb: {
            parent: 'data.list',
            label: '{{$state.params.streamId | caskCapitalizeFilter}}'
          }
        })
          .state('streams.detail.overview.tab', {
            url: '/:tab',
            ncyBreadcrumb: {
              skip: true
            }
          });
});
