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
            skip: true
          }
        })
        .state('flows.detail', {
          url: '/:programId',
          templateUrl: '/assets/features/flows/templates/detail.html',
          controller: 'FlowsDetailController',
          ncyBreadcrumb: {
            parent: 'apps.detail.overview',
            label: '{{$state.params.programId | caskCapitalizeFilter}}'
          }
        })
          .state('flows.detail.runs', {
            url: '/runs',
            templateUrl: '/assets/features/flows/templates/tabs/runs.html',
            controller: 'FlowsDetailRunController',
            ncyBreadcrumb: {
              skip: true
            }
          })

            .state('flows.detail.runs.detail', {
              url: '/:runId',
              template: '<ui-view/>',
              abstract: true
            })
              .state('flows.detail.runs.detail.status', {
                url: '/status',
                template: '<div> Status: {{$state.params.runId}} </div>',
                ncyBreadcrumb: {
                  skip: true
                }
              })
              .state('flows.detail.runs.detail.flowlets', {
                url: '/flowlets',
                template: '<div> Flowlets: {{$state.params.runId}} </div>',
                ncyBreadcrumb: {
                  skip: true
                }
              })
              .state('flows.detail.runs.detail.data', {
                url: '/data',
                template: '<div> Data: {{$state.params.runId}} </div>',
                ncyBreadcrumb: {
                  skip: true
                }
              })
              .state('flows.detail.runs.detail.configuration', {
                url: '/configuration',
                template: '<div> Configuration: {{$state.params.runId}} </div>',
                ncyBreadcrumb: {
                  skip: true
                }
              })
              .state('flows.detail.runs.detail.log', {
                url: '/log',
                template: '<div> Log: {{$state.params.runId}} </div>',
                ncyBreadcrumb: {
                  skip: true
                }
              })
          .state('flows.detail.schedules', {
            url: '/schedules',
            templateUrl: '/assets/features/flows/templates/tabs/schedules.html'
          })
          .state('flows.detail.metadata', {
            url: '/metadata',
            templateUrl: '/assets/features/flows/templates/tabs/metadata.html'
          })
          .state('flows.detail.history', {
            url: '/history',
            templateUrl: '/assets/features/flows/templates/tabs/history.html'
          })
          .state('flows.detail.resources', {
            url: '/resources',
            templateUrl: '/assets/features/flows/templates/tabs/resources.html'
          });
  });
