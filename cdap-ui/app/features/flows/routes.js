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
        resolve : {
          rRuns: function($stateParams, $q, myFlowsApi) {
            var defer = $q.defer();

            // Using _cdapPath here as $state.params is not updated with
            // runid param when the request goes out
            // (timing issue with re-direct from login state).
            var params = {
              namespace: $stateParams.namespace,
              appId: $stateParams.appId,
              flowId: $stateParams.programId
            };
            myFlowsApi.runs(params)
              .$promise
              .then(function (res) {
                defer.resolve(res);
              });
            return defer.promise;
          }

        },
        ncyBreadcrumb: {
          parent: 'apps.detail.overview.status',
          label: 'Flows',
          skip: true
        },
        templateUrl: '/assets/features/flows/templates/detail.html'
      })

      .state('flows.detail.runs', {
        url: '/runs',
        templateUrl: '/assets/features/flows/templates/tabs/runs.html',
        controller: 'FlowsRunsController',
        controllerAs: 'RunsController',
        ncyBreadcrumb: {
          label: '{{$state.params.programId}}'
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

      .state('flows.detail.datasets', {
        url: '/data',
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'development'
        },
        templateUrl: '/assets/features/flows/templates/tabs/data.html',
        ncyBreadcrumb: {
          label: 'Datasets',
          parent: 'flows.detail.runs'
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
        controllerAs: 'RunsController',
        ncyBreadcrumb: {
          label: 'History',
          parent: 'flows.detail.runs'
        }
      });

  });
