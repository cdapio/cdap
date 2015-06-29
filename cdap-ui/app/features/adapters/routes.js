angular.module(PKG.name + '.feature.adapters')
  .config(function($stateProvider, $urlRouterProvider, MYAUTH_ROLE) {
    $stateProvider
      .state('adapters', {
        url: '/adapters',
        abstract: true,
        parent: 'apps',
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'development'
        },
        template: '<ui-view/>'
      })

        .state('adapters.list', {
          url: '',
          templateUrl: '/assets/features/adapters/templates/list.html',
          controller: 'AdapterListController',
          ncyBreadcrumb: {
            label: 'Adapters',
            parent: 'overview'
          }
        })

        .state('adapters.create', {
          url: '/create',
          params: {
            data: null
          },
          controller: 'AdapterCreateController as AdapterCreateController',
          templateUrl: '/assets/features/adapters/templates/create.html',
          ncyBreadcrumb: {
            skip: true
          }
        })

        .state('adapters.detail', {
          url: '/:adapterId',
          data: {
            authorizedRoles: MYAUTH_ROLE.all,
            highlightTab: 'development'
          },
          resolve : {
            rRuns: function($stateParams, $q, myAdapterApi) {
              var defer = $q.defer();
              // Using _cdapPath here as $state.params is not updated with
              // runid param when the request goes out
              // (timing issue with re-direct from login state).
              var params = {
                namespace: $stateParams.namespace,
                adapter: $stateParams.adapterId
              };

              myAdapterApi.runs(params)
                .$promise
                .then(function(res) {
                  defer.resolve(res);
                });
              return defer.promise;
            },
            rAdapterDetail: function($stateParams, $q, myAdapterApi) {
              var params = {
                namespace: $stateParams.namespace,
                adapter: $stateParams.adapterId
              };

              return myAdapterApi.get(params).$promise;
            }
          },
          ncyBreadcrumb: {
            parent: 'adapters.list',
            label: '{{$state.params.adapterId}}'
          },
          templateUrl: '/assets/features/adapters/templates/detail.html',
          controller: 'AdpaterDetailController'
        })
          .state('adapters.detail.runs',{
            url: '/runs',
            templateUrl: '/assets/features/adapters/templates/tabs/runs.html',
            controller: 'AdapterRunsController',
            ncyBreadcrumb: {
              parent: 'adapters.list',
              label: '{{$state.params.adapterId}}'
            }
          })
            .state('adapters.detail.runs.run', {
              url: '/:runid',
              templateUrl: '/assets/features/adapters/templates/tabs/runs/run-detail.html',
              ncyBreadcrumb: {
                label: '{{$state.params.runid}}'
              }
            })

        .state('adapters.detail.datasets', {
          url: '/datasets',
          data: {
            authorizedRoles: MYAUTH_ROLE.all,
            highlightTab: 'development'
          },
          templateUrl: 'data-list/data-list.html',
          controller: 'AdapterDatasetsController',
          ncyBreadcrumb: {
            label: 'Datasets',
            parent: 'adapters.detail.runs'
          }
        })
        .state('adapters.detail.history', {
          url: '/history',
          data: {
            authorizedRoles: MYAUTH_ROLE.all,
            highlightTab: 'development'
          },
          templateUrl: '/assets/features/adapters/templates/tabs/history.html',
          controller: 'AdapterRunsController',
          ncyBreadcrumb: {
            label: 'History',
            parent: 'adapters.detail.runs'
          }
        });
  });
