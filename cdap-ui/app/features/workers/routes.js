angular.module(PKG.name + '.feature.worker')
  .config(function($stateProvider, $urlRouterProvider, MYAUTH_ROLE) {
    $stateProvider
      .state('worker', {
        url: '/workers/:programId',
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
              _cdapPath: '/namespaces/' + $stateParams.namespace +
                         '/apps/' + $stateParams.appId +
                         '/workers/' + $stateParams.programId +
                         '/runs'
            })
              .then(function(res) {
                defer.resolve(res);
              });
            return defer.promise;
          }
        },
        template: '<ui-view/>'
      })

      .state('worker.detail', {
        url: '/runs',
        templateUrl: '/assets/features/workers/templates/detail.html',
        controller: 'WorkersRunsController',
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

      .state('worker.detail.run', {
        url: '/:runid',
        templateUrl: '/assets/features/workers/templates/tabs/runs/run-detail.html',
        controller: 'WorkersRunDetailController',
        controllerAs: 'RunsDetailController',
        ncyBreadcrumb: {
          label: '{{$state.params.runid}}',
          parent: 'worker.detail'
        }
      });
  });
