angular.module(PKG.name + '.feature.worker')
  .config(function($stateProvider, $urlRouterProvider, MYAUTH_ROLE) {
    $stateProvider
      .state('worker', {
        url: '/workers',
        abstract: true,
        parent: 'programs',
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'development'
        },
        template: '<ui-view/>'
      })

      .state('worker.detail', {
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
                         '/workers/' + $stateParams.programId +
                         '/runs'
            })
              .then(function(res) {
                defer.resolve(res);
              });
            return defer.promise;
          }

        },
        ncyBreadcrumb: {
          parent: 'apps.detail.overview.status',
          label: 'Workers',
          skip: true
        },
        templateUrl: '/assets/features/workers/templates/detail.html',
        controller: 'WorkersDetailController'
      })

      .state('worker.detail.runs', {
        url: '/runs',
        templateUrl: '/assets/features/workers/templates/tabs/runs.html',
        controller: 'WorkersRunsController',
        ncyBreadcrumb: {
          label: '{{$state.params.programId}}'
        }
      })
        .state('worker.detail.runs.run', {
          url: '/:runid',
          templateUrl: '/assets/features/workers/templates/tabs/runs/run-detail.html',
          controller: 'WorkersRunDetailController',
          ncyBreadcrumb: {
            label: '{{$state.params.runid}}'
          }
        })


      .state('worker.detail.history', {
        url: '/history',
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'development'
        },
        templateUrl: '/assets/features/workers/templates/tabs/history.html',
        controller: 'WorkersRunsController',
        ncyBreadcrumb: {
          label: 'History',
          parent: 'worker.detail.runs'
        }
      })

  });
