angular.module(PKG.name + '.feature.mapreduce')
  .config(function($stateProvider, $urlRouterProvider, MYAUTH_ROLE) {
    $stateProvider
      .state('mapreduce', {
        url: '/mapreduce',
        abstract: true,
        parent: 'programs',
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'development'
        },
        template: '<ui-view/>'
      })

      .state('mapreduce.detail', {
        url: '/:programId',
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'development'
        },
        templateUrl: '/assets/features/mapreduce/templates/detail.html',
        controller: 'MapreduceDetailController',
        ncyBreadcrumb: {
          parent: 'apps.detail.overview.status',
          label: 'Mapreduce',
          skip: true
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
          }
        }
      })
        .state('mapreduce.detail.runs', {
          url: '/runs',
          templateUrl: '/assets/features/mapreduce/templates/tabs/runs.html',
          controller: 'MapreduceRunsController',
          ncyBreadcrumb: {
            label: '{{$state.params.programId}}'
          }
        })
          .state('mapreduce.detail.runs.run', {
            url: '/:runid',
            templateUrl: '/assets/features/mapreduce/templates/tabs/runs/run-detail.html',
            controller: 'MapreduceRunsDetailController',
            ncyBreadcrumb: {
              label: '{{ $state.params.runid }}'
            }
          })
        .state('mapreduce.detail.datasets', {
          url: '/data',
          templateUrl: '/assets/features/mapreduce/templates/tabs/data.html',
          ncyBreadcrumb: {
            parent: 'mapreduce.detail.runs',
            label: 'Datasets'
          }
        })
        .state('mapreduce.detail.history', {
          url: '/history',
          templateUrl: '/assets/features/mapreduce/templates/tabs/history.html',
          controller: 'MapreduceRunsController',
          ncyBreadcrumb: {
            parent: 'mapreduce.detail.runs',
            label: 'History'
          }
        });
  });
