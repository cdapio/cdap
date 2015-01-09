angular.module(PKG.name + '.feature.programs')
  .config(function($stateProvider, $urlRouterProvider, MYAUTH_ROLE) {
    $stateProvider
      .state('programs', {
        parent: 'apps.detail',
        url: '/programs/:programType',
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'development'
        },
        template: '<ui-view/>',
        abstract: true
      })
        .state('programs.type', {
          url: '',
          templateUrl: '/assets/features/programs/templates/list.html',
          controller: 'ProgramsListController',
          ncyBreadcrumb: {
            label: '{{$state.params.programType}}',
            parent: 'apps.detail.overview'
          }
        })

        // .state('programs.detail' {
        //   url: '/:programTypeId',
        //   abstract: true,
        //   template: '<ui-view/>'
        // })
        //   .state('program-overview', {
        //     url: '/overview',
        //     parent: 'programs.detail',
        //     templateUrl: 'assets/features/programs/templates/detail.html',
        //     controller: 'ProgramsDetailController',
        //     ncyBreadcrumb: {
        //       label: '{{$state.params.programTypeId}}',
        //       parent: 'programs.type'
        //     }
        //   })
        //   .state('programs.detail.tab', {
        //     url: '/:tab',
        //     ncyBreadcrumb: {
        //       parent: 'programs.detail',
        //       label: '{{$state.params.programTabId}}'
        //     }
        //   })
  });
