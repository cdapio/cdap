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
          controller: 'FlowsListController',
          ncyBreadcrumb: {
            skip: true
          }
        })
        .state('flows.detail', {
          url: '/:programId',
          templateUrl: '/assets/features/flows/templates/detail.html',
          controller: 'FlowsDetailController',
          onEnter: function($state, $timeout) {

            $timeout(function() {
              if ($state.is('flows.detail')) {
                $state.go('flows.detail.runs');
              }
            });

          },
          ncyBreadcrumb: {
            parent: 'apps.detail.overview',
            label: '{{$state.params.programId }}'
          }
        })
          .state('flows.detail.runs', {
            url: '',
            templateUrl: '/assets/features/flows/templates/tabs/runs.html',
            controller: 'FlowsDetailRunController',
            ncyBreadcrumb: {
              skip: true
            }
          })

            .state('flows.detail.runs.tabs', {
              url: '/runs/:runId',
              template: '<ui-view/>',
              abstract: true
            })
              .state('flows.detail.runs.tabs.status', {
                url: '/status',
                templateUrl: '/assets/features/flows/templates/tabs/runs/status.html',
                controller: 'FlowsDetailRunStatusController',
                ncyBreadcrumb: {
                  parent: 'apps.detail.overview',
                  label: '{{$state.params.programId}} / {{$state.params.runId}}'
                }
              })
              .state('flows.detail.runs.tabs.status.flowletsDetail', {
                url: '/:flowletId',
                templateUrl: '/assets/features/flows/templates/tabs/runs/flowlets/detail.html',
                controller: 'FlowsFlowletDetailController',
                ncyBreadcrumb: {
                  skip: true
                }
              })
              .state('flows.detail.runs.tabs.status.streamsDetail', {
                url: '/:streamId', // Doesn't work with ngStrap
                onEnter: function ($rootScope, $stateParams, $state, $modal) {
                  var scope = $rootScope.$new();
                  scope.streamId = $stateParams.streamId;
                  $modal({
                    template: '/assets/features/flows/templates/tabs/runs/streams/detail.html',
                    scope: scope
                  }).$promise.then(function () {
                    $state.go('^', $state.params);
                  });
                },
                ncyBreadcrumb: {
                  skip: true
                }
              })
              .state('flows.detail.runs.tabs.data', {
                url: '/data',
                templateUrl: '/assets/features/flows/templates/tabs/runs/data.html',
                ncyBreadcrumb: {
                  skip: true
                }
              })
              .state('flows.detail.runs.tabs.configuration', {
                url: '/configuration',
                templateUrl: '/assets/features/flows/templates/tabs/runs/configuration.html',
                ncyBreadcrumb: {
                  skip: true
                }
              })
              .state('flows.detail.runs.tabs.log', {
                url: '/logs?filter',
                reloadOnSearch: false,
                controller: 'FlowsRunLogController',
                template: '<my-log-viewer data-model="logs"></my-log-viewer>',
                ncyBreadcrumb: {
                  skip: true
                }
              })
          .state('flows.detail.schedules', {
            url: '/schedules',
            templateUrl: '/assets/features/flows/templates/tabs/schedules.html',
            ncyBreadcrumb: {
              skip: true
            }
          })
          .state('flows.detail.metadata', {
            url: '/metadata',
            templateUrl: '/assets/features/flows/templates/tabs/metadata.html',
            ncyBreadcrumb: {
              skip: true
            }
          })
          .state('flows.detail.history', {
            url: '/history',
            templateUrl: '/assets/features/flows/templates/tabs/history.html',
            ncyBreadcrumb: {
              parent: 'apps.detail.overview',
              label: '{{$state.params.programId}} / History'
            }
          })
          .state('flows.detail.resources', {
            url: '/resources',
            templateUrl: '/assets/features/flows/templates/tabs/resources.html',
            ncyBreadcrumb: {
              skip: true
            }
          });
  });
