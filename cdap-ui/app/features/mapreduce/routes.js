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
          controllerAs: 'RunsController',
          ncyBreadcrumb: {
            label: '{{$state.params.programId}}'
          }
        })
          .state('mapreduce.detail.runs.run', {
            url: '/:runid?sourceId&sourceRunId&destinationType',
            templateUrl: '/assets/features/mapreduce/templates/tabs/runs/run-detail.html',
            controller: 'MapreduceRunsDetailController',
            ncyBreadcrumb: {
              label: '{{ $state.params.runid }}'
            }
          })
        .state('mapreduce.detail.datasets', {
          url: '/data',
          template: '<my-data-list data-level="program" data-program="mapreduce"></my-data-list>',
          ncyBreadcrumb: {
            parent: 'mapreduce.detail.runs',
            label: 'Datasets'
          }
        })
        .state('mapreduce.detail.history', {
          url: '/history',
          template: '<my-program-history data-runs="RunsController.runs" data-type="MAPREDUCE"></my-program-history>',
          controller: 'MapreduceRunsController',
          controllerAs: 'RunsController',
          ncyBreadcrumb: {
            parent: 'mapreduce.detail.runs',
            label: 'History'
          }
        });
  });
