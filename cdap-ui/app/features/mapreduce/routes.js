angular.module(PKG.name + '.feature.mapreduce')
  .config(function($stateProvider, $urlRouterProvider, MYAUTH_ROLE) {
    $stateProvider
      .state('mapreduce', {
        url: '/mapreduce/:programId',
        abstract: true,
        parent: 'programs',
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'development'
        },
        resolve: {
          rRuns: function($stateParams, $q, myMapreduceApi) {
            var defer = $q.defer();

            var params = {
              namespace: $stateParams.namespace,
              appId: $stateParams.appId,
              mapreduceId: $stateParams.programId
            };
            myMapreduceApi.runs(params)
              .$promise
              .then(function (res) {
                defer.resolve(res);
              });

            return defer.promise;
          },
          rMapreduceDetail: function($stateParams, myMapreduceApi) {
            var params = {
              namespace: $stateParams.namespace,
              appId: $stateParams.appId,
              mapreduceId: $stateParams.programId
            };
            return myMapreduceApi.get(params).$promise;
          }
        },
        template: '<ui-view/>'
      })

      .state('mapreduce.detail', {
        url: '/runs',
        templateUrl: '/assets/features/mapreduce/templates/detail.html',
        controller: 'MapreduceRunsController',
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
        .state('mapreduce.detail.run', {
          url: '/:runid?sourceId&sourceRunId&destinationType',
          templateUrl: '/assets/features/mapreduce/templates/tabs/runs/run-detail.html',
          controller: 'MapreduceRunsDetailController',
          controllerAs: 'RunsDetailController',
          ncyBreadcrumb: {
            label: '{{$state.params.runid}}',
            parent: 'mapreduce.detail'
          }
        });

  });
