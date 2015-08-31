angular.module(PKG.name + '.feature.flows')
  .config(function($stateProvider, $urlRouterProvider, MYAUTH_ROLE) {
    $stateProvider
      .state('flows', {
        url: '/flows/:programId',
        abstract: true,
        parent: 'programs',
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
        template: '<ui-view/>'
      })

      .state('flows.detail', {
        url: '/runs',
        templateUrl: '/assets/features/flows/templates/detail.html',
        controller: 'FlowsRunsController',
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

      .state('flows.detail.run', {
        url: '/:runid',
        templateUrl: '/assets/features/flows/templates/tabs/runs/run-detail.html',
        controller: 'FlowsRunDetailController',
        ncyBreadcrumb: {
          label: '{{$state.params.runid}}',
          parent: 'flows.detail'
        }
      });
  });
