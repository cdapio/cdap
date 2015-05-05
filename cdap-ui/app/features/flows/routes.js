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

      .state('flows.detail.runs', {
        url: '/runs',
        templateUrl: '/assets/features/flows/templates/tabs/runs.html',
        controller: 'FlowsRunsController',
        ncyBreadcrumb: {
          label: '{{$state.params.programId}}'
        }
      })
        .state('flows.detail.runs.run', {
          url: '/:runid',
          templateUrl: '/assets/features/flows/templates/tabs/runs/run-detail.html',
          ncyBreadcrumb: {
            label: '{{$state.params.runid}}'
          }
        })

      .state('flows.detail.data', {
        url: '/data',
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'development'
        },
        templateUrl: '/assets/features/flows/templates/tabs/data.html',
        ncyBreadcrumb: {
          label: 'Data'
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
      });

  });
