angular.module(PKG.name + '.feature.spark')
  .config(function($stateProvider, $urlRouterProvider, MYAUTH_ROLE) {
    $stateProvider
      .state('spark', {
        url: '/spark/:programId',
        abstract: true,
        parent: 'programs',
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'development'
        },
        resolve : {
          rRuns: function($stateParams, $q, mySparkApi) {
            var defer = $q.defer();

            var params = {
              namespace: $stateParams.namespace,
              appId: $stateParams.appId,
              sparkId: $stateParams.programId
            };
            mySparkApi.runs(params)
              .$promise
              .then(function (res) {
                defer.resolve(res);
              });

            return defer.promise;
          },
          rSparkDetail: function($stateParams, mySparkApi) {
            var params = {
              namespace: $stateParams.namespace,
              appId: $stateParams.appId,
              sparkId: $stateParams.programId
            };
            return mySparkApi.get(params).$promise;
          }
        },
        template: '<ui-view/>'
      })

      .state('spark.detail', {
        url: '/runs',
        templateUrl: '/assets/features/spark/templates/detail.html',
        controller: 'SparkRunsController',
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

      .state('spark.detail.run', {
        url: '/:runid?sourceId&sourceRunId&destinationType',
        templateUrl: '/assets/features/spark/templates/tabs/runs/run-detail.html',
        controller: 'SparkRunDetailController',
        controllerAs: 'RunsDetailController',
        ncyBreadcrumb: {
          label: '{{$state.params.runid}}',
          parent: 'spark.detail'
        }
      });

  });
