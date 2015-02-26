angular.module(PKG.name + '.feature.workflows')
  .config(function($stateProvider, $urlRouterProvider, MYAUTH_ROLE) {
    $stateProvider
      .state('workflows', {
        url: '/workflows',
        abstract: true,
        parent: 'programs',
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'development'
        },
        template: '<ui-view/>'
      })
        .state('workflows.list', {
          url: '/list',
          templateUrl: '/assets/features/workflows/templates/list.html',
          controller: 'WorkflowsListController',
          ncyBreadcrumb: {
            skip: true
          }
        })
        .state('workflows.detail', {
          url: '/:programId',
          templateUrl: '/assets/features/workflows/templates/detail.html',
          controller: 'WorkflowsDetailController',
          onEnter: function($state, $timeout) {

            $timeout(function() {
              if ($state.is('workflows.detail')) {
                $state.go('workflows.detail.runs');
              }
            });

          }
        })
          .state('workflows.detail.runs', {
            url: '',
            templateUrl: '/assets/features/workflows/templates/tabs/runs.html',
            controller: 'WorkflowsDetailRunController',
            ncyBreadcrumb: {
              parent: 'apps.detail.overview',
              label: '{{$state.params.programId}}'
            }
          })

            .state('workflows.detail.runs.tabs', {
              url: '/runs/:runId',
              template: '<ui-view/>',
              abstract: true,
              ncyBreadcrumb: {
                skip: true
              }
            })
              .state('workflows.detail.runs.tabs.status', {
                url: '/status',
                templateUrl: '/assets/features/workflows/templates/tabs/runs/flow.html',
                controller: 'WorkflowsDetailRunStatusController',
                ncyBreadcrumb: {
                  parent: 'apps.detail.overview',
                  label: '{{$state.params.programId}} / {{$state.params.runId}}'
                }
              })

              .state('workflows.detail.runs.tabs.data', {
                url: '/data',
                template:
                '<div class="well well-lg text-center">' +
                  '<div> Workflow: Data - Work In Progress</div> ' +
                '</div>',
                ncyBreadcrumb: {
                  parent: 'apps.detail.overview',
                  label: '{{$state.params.programId}} / {{$state.params.runId}}'
                }
              })
              .state('workflows.detail.runs.tabs.configuration', {
                url: '/configuration',
                template:
                '<div class="well well-lg text-center">' +
                  '<div> Workflow: Configuration - Work In Progress</div> ' +
                '</div>',
                ncyBreadcrumb: {
                  parent: 'apps.detail.overview',
                  label: '{{$state.params.programId}} / {{$state.params.runId}}'
                }
              })
              .state('workflows.detail.runs.tabs.log', {
                url: '/log',
                template:
                '<div class="well well-lg text-center">' +
                  '<div> Workflow: Log - Work In Progress</div> ' +
                '</div>',
                ncyBreadcrumb: {
                  parent: 'apps.detail.overview',
                  label: '{{$state.params.programId}} / {{$state.params.runId}}'
                }
              })

          .state('workflows.detail.schedules', {
            url: '/schedules',
            templateUrl: '/assets/features/workflows/templates/tabs/schedules.html',
            ncyBreadcrumb: {
              parent: 'apps.detail.overview',
              label: '{{$state.params.programId}}'
            }
          })
          .state('workflows.detail.metadata', {
            url: '/metadata',
            templateUrl: '/assets/features/workflows/templates/tabs/metadata.html',
            ncyBreadcrumb: {
              parent: 'apps.detail.overview',
              label: '{{$state.params.programId}}'
            }
          })
          .state('workflows.detail.history', {
            url: '/history',
            templateUrl: '/assets/features/workflows/templates/tabs/history.html',
            ncyBreadcrumb: {
              parent: 'apps.detail.overview',
              label: '{{$state.params.programId}} / History'
            }
          })
          .state('workflows.detail.resources', {
            url: '/resources',
            templateUrl: '/assets/features/workflows/templates/tabs/resources.html',
            ncyBreadcrumb: {
              parent: 'apps.detail.overview',
              label: '{{$state.params.programId}}'
            }
          });
  });
