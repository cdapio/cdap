angular.module(`${PKG.name}.feature.workflows`)
  .config(function($stateProvider, $urlRouterProvider, MYAUTH_ROLE) {
    $stateProvider
      .state('workflows', {
        url: '/workflows/:programId',
        abstract: true,
        parent: 'programs',
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'development'
        },
        resolve : {
          rRuns: function(MyDataSource, $stateParams, $q) {
            var defer = $q.defer();
            var dataSrc = new MyDataSource();
            // Using _cdapPath here as $state.params is not updated with
            // runid param when the request goes out
            // (timing issue with re-direct from login state).
            dataSrc.request({
              _cdapPath: `/namespaces/${$stateParams.namespace}/apps/${$stateParams.appId}/workflows/${$stateParams.programId}/runs`
            })
              .then(function(res) {
                defer.resolve(res);
              });
            return defer.promise;
          }
        },
        template: '<ui-view/>'
      })

        .state('workflows.detail', {
          url: '/runs',
          templateUrl: '/assets/features/workflows/templates/detail.html',
          controller: 'WorkflowsRunsController',
          controllerAs: 'RunsController',
          data: {
            authorizedRoles: MYAUTH_ROLE.all,
            highlightTab: 'development'
          },
          ncyBreadcrumb: {
            parent: 'apps.detail.overview.status',
            label: '{{$state.params.programId}}'
          }
        })

          .state('workflows.detail.run', {
            url: '/:runid',
            templateUrl: '/assets/features/workflows/templates/tabs/runs/run-detail.html',
            controller: 'WorkflowsRunsDetailController',
            controllerAs: 'RunsDetailController',
            ncyBreadcrumb: {
              label: '{{$state.params.runid}}',
              parent: 'workflows.detail'
            }
          })
          .state('workflows.detail.history', {
            url: '/history',
            data: {
              authorizedRoles: MYAUTH_ROLE.all,
              highlightTab: 'development'
            },
            ncyBreadcrumb: {
              parent: 'workflows.detail.runs',
              label: 'History'
            },
            template: '<my-program-history data-runs="RunsController.runs" data-type="WORKFLOWS"></my-program-history>',
            controller: 'WorkflowsRunsController',
            controllerAs: 'RunsController'
          })
          .state('workflows.detail.schedules', {
            url: '/schedules',
            data: {
              authorizedRoles: MYAUTH_ROLE.all,
              highlightTab: 'development'
            },
            ncyBreadcrumb: {
              label: 'Schedules',
              parent: 'workflows.detail.runs'
            },
            templateUrl: '/assets/features/workflows/templates/tabs/schedules.html',
            controller: 'WorkflowsSchedulesController',
            controllerAs: 'SchedulesController'
          });
  });
