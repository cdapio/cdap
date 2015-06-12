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
          controllerAs: 'DetailController',
          ncyBreadcrumb: {
            parent: 'data.list',
            label: '{{$state.params.streamId}}'
          }
        })

        .state('streams.detail.overview.status', {
          url: '/status',
          templateUrl: '/assets/features/streams/templates/tabs/status.html',
          ncyBreadcrumb: {
            parent: 'data.list',
            label: '{{$state.params.streamId}}'
          },
          controller: 'StreamDetailStatusController',
          controllerAs: 'StatusController'
        })

        .state('streams.detail.overview.explore', {
          url: '/explore',
          templateUrl: '/assets/features/streams/templates/tabs/explore.html',
          controller: 'StreamExploreController',
          controllerAs: 'ExploreController',
          ncyBreadcrumb: {
            label: 'Explore',
            parent: 'streams.detail.overview.status'
          }
        })

        .state('streams.detail.overview.programs', {
          url: '/programs',
          templateUrl: '/assets/features/streams/templates/tabs/programs.html',
          controller: 'StreamProgramsController',
          controllerAs: 'ProgramsController',
          ncyBreadcrumb: {
            label: 'Programs',
            parent: 'streams.detail.overview.status'
          }
        })

        .state('streams.detail.overview.metadata', {
          url: '/metadata',
          templateUrl: '/assets/features/streams/templates/tabs/metadata.html',
          controller: 'StreamMetadataController',
          controllerAs: 'MetadataController',
          ncyBreadcrumb: {
            label: 'Metadata',
            parent: 'streams.detail.overview.status'
          }
        });
});
