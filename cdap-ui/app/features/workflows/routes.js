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
          },
          rWorkflowDetail: function(myWorkFlowApi, $stateParams) {
            var params = {
              namespace: $stateParams.namespace,
              appId: $stateParams.appId,
              workflowId: $stateParams.programId
            };
            return myWorkFlowApi.get(params).$promise;
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
          },
          onExit: function($modalStack) {
            $modalStack.dismissAll();
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
            },
            onExit: function($modalStack) {
              $modalStack.dismissAll();
            }
          });
  });
