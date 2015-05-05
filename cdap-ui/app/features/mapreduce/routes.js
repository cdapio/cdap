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
          parent: 'apps.detail.overview',
          label: 'Mapreduce',
          skip: true
        }
      })
        .state('mapreduce.detail.runs', {
          url: '/runs',
          templateUrl: '/assets/features/mapreduce/templates/tabs/runs.html',
          controller: 'MapreduceRunsController',
          ncyBreadcrumb: {
            label: '{{$state.params.programId}}'
          },
          resolve: {
            rRuns: function(MyDataSource, $stateParams, $q) {
              var defer = $q.defer();

              var dataSrc = new MyDataSource();

              dataSrc.request({
                _cdapPath: '/namespaces/' + $stateParams.namespace + '/apps/' + $stateParams.appId + '/mapreduce/' + $stateParams.programId + '/runs'
              })
              .then(function (res) {
                defer.resolve(res);
              });

              return defer.promise;
            }
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
        .state('mapreduce.detail.data', {
          url: '/data',
          templateUrl: '/assets/features/mapreduce/templates/tabs/data.html',
          ncyBreadcrumb: {
            parent: 'apps.detail.overview',
            label: 'Data'
          }
        })
        .state('mapreduce.detail.history', {
          url: '/history',
          templateUrl: '/assets/features/mapreduce/templates/tabs/history.html',
          ncyBreadcrumb: {
            parent: 'apps.detail.overview',
            label: 'History'
          }
        });
  });
