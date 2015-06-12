angular.module(PKG.name + '.feature.spark')
  .config(function($stateProvider, $urlRouterProvider, MYAUTH_ROLE) {
    $stateProvider
      .state('spark', {
        url: '/spark',
        abstract: true,
        parent: 'programs',
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'development'
        },
        template: '<ui-view/>'
      })

      .state('spark.detail', {
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
                         '/spark/' + $stateParams.programId +
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
          label: 'Spark',
          skip: true
        },
        templateUrl: '/assets/features/spark/templates/detail.html',
        controller: 'SparkDetailController'
      })

      .state('spark.detail.runs', {
        url: '/runs',
        templateUrl: '/assets/features/spark/templates/tabs/runs.html',
        controller: 'SparkRunsController',
        ncyBreadcrumb: {
          label: '{{$state.params.programId}}'
        }
      })
        .state('spark.detail.runs.run', {
          url: '/:runid',
          templateUrl: '/assets/features/spark/templates/tabs/runs/run-detail.html',
          controller: 'SparkRunDetailController',
          ncyBreadcrumb: {
            label: '{{$state.params.runid}}'
          }
        })


      .state('spark.detail.history', {
        url: '/history',
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'development'
        },
        templateUrl: '/assets/features/spark/templates/tabs/history.html',
        controller: 'SparkRunsController',
        ncyBreadcrumb: {
          label: 'History',
          parent: 'spark.detail.runs'
        }
      });

  });
