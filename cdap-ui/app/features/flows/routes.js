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

      .state('flows.detail', {
        url: '/:programId',
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'development'
        },
        ncyBreadcrumb: {
          parent: 'apps.detail.overview',
          label: 'Flows',
          skip: true
        },
        templateUrl: '/assets/features/flows/templates/detail.html',
        controller: 'FlowsDetailController'
      })
        .state('flows.detail.flowlet', {
          url: '/flowlet/:flowletid',
          templateUrl: '/assets/features/flows/templates/tabs/runs/flowlets/detail.html',
          controller: 'FlowsFlowletDetailController'
        })

      .state('flows.detail.status', {
        url: '/status',
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'development'
        },
        ncyBreadcrumb: {
          label: '{{$state.params.programId}}'
        },
        templateUrl: '/assets/features/flows/templates/tabs/status.html',
        controller: 'FlowsDetailController'
      })

      .state('flows.detail.runs', {
        url: '/runs',
        templateUrl: '/assets/features/flows/templates/tabs/runs.html',
        controller: 'FlowsRunsController',
        ncyBreadcrumb: {
          label: 'Runs'
        }
      })
        .state('flows.detail.runs.run', {
          url: '/:runid',
          templateUrl: '/assets/features/flows/templates/tabs/runs/run-detail.html',
          controller: 'FlowsRunDetailController',
          ncyBreadcrumb: {
            label: '{{$state.params.runid}}'
          }
        })

      .state('flows.detail.history', {
        url: '/history',
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'development'
        },
        templateUrl: '/assets/features/flows/templates/tabs/history.html',
        controller: 'FlowsRunsController',
        ncyBreadcrumb: {
          label: 'History'
        }
      })

  });
