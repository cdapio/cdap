angular.module(PKG.name + '.feature.programs')
  .config(function($stateProvider, $urlRouterProvider, MYAUTH_ROLE) {
    $stateProvider
      .state('cdap-programs', {
        parent: 'apps.detail',
        url: '/programs/:programType',
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'development'
        },
        template: '<ui-view/>',
        abstract: true
      })
        .state('cdap-programs.type', {
          url: '',
          templateUrl: '/assets/features/programs/templates/list.html',
          controller: 'ProgramsListController',
          ncyBreadcrumb: {
            label: '{{$state.params.programType}}',
            parent: 'app-overview'
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
        //       parent: 'cdap-programs.type'
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
