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
          rRuns: function(MyDataSource, $stateParams, $q) {
            var defer = $q.defer();
            var dataSrc = new MyDataSource();
            // Using _cdapPath here as $state.params is not updated with
            // runid param when the request goes out
            // (timing issue with re-direct from login state).
            dataSrc.request({
              _cdapPath: '/namespaces/' + $stateParams.namespace +
                         '/apps/' + $stateParams.appId +
                         '/flows/' + $stateParams.programId +
                         '/runs'
            })
              .then(function(res) {
                defer.resolve(res);
              });
            return defer.promise;
          }
        },
        ncyBreadcrumb: {
          parent: 'apps.detail.overview',
          label: 'Flows',
          skip: true
        },
        templateUrl: '/assets/features/flows/templates/detail.html',
        controller: 'FlowsDetailController'
      })
        .state('flows.detail.flowlets', {
          url: '/flowlets',
          templateUrl: '/assets/features/flows/templates/tabs/flowlets.html',
          controller: 'FlowletsController',
          ncyBreadcrumb: {
            label: 'Flowlets'
          }
        })
          .state('flows.detail.flowlets.flowlet', {
            url: '/:flowletid',
            templateUrl: '/assets/features/flows/templates/tabs/runs/flowlets/detail.html',
            controller: 'FlowsFlowletDetailController',
            ncyBreadcrumb:{
              label: '{{$state.params.flowletid}}'
            }
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
