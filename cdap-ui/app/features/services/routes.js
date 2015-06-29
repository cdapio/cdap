angular.module(PKG.name + '.feature.services')
  .config(function($stateProvider, $urlRouterProvider, MYAUTH_ROLE) {
    $stateProvider
      .state('services', {
        url: '/services',
        abstract: true,
        parent: 'programs',
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'development'
        },
        template: '<ui-view/>'
      })
      .state('services.detail', {
        url: '/:programId',
        templateUrl: '/assets/features/services/templates/detail.html',
        resolve : {
          rRuns: function($stateParams, $q, myServiceApi) {
            var defer = $q.defer();

            var params = {
              namespace: $stateParams.namespace,
              appId: $stateParams.appId,
              serviceId: $stateParams.programId
            };
            myServiceApi.runs(params)
              .$promise
              .then(function (res) {
                defer.resolve(res);
              });

            return defer.promise;
          }
        },
        ncyBreadcrumb: {
          parent: 'apps.detail.overview.status',
          label: 'Services',
          skip: true
        }
      })
        .state('services.detail.runs', {
          url: '/runs',
          templateUrl: '/assets/features/services/templates/tabs/runs.html',
          controller: 'ServicesRunsController',
          controllerAs: 'RunsController',
          ncyBreadcrumb: {
            label: '{{$state.params.programId}}'
          }
        })
          .state('services.detail.runs.run', {
            url: '/:runid',
            templateUrl: '/assets/features/services/templates/tabs/runs/run-detail.html',
            controller: 'ServicesRunDetailController',
            ncyBreadcrumb: {
              label: '{{$state.params.runid}}'
            }
          })
          .state('services.detail.runs.makerequest', {
            params: {
              requestUrl: null,
              requestMethod: null
            },
            onEnter: function ($state, $modal) {
              var modal = $modal({
                template: '/assets/features/services/templates/tabs/runs/tabs/status/make-request.html',
              });
              modal.$scope.$on('modal.hide', function() {
                $state.go('^');
              });
            }
          })


        .state('services.detail.datasets', {
          url: '/data',
          templateUrl: '/assets/features/services/templates/tabs/data.html',
          ncyBreadcrumb: {
            parent: 'services.detail.runs',
            label: 'Datasets'
          }
        })
        .state('services.detail.history', {
          url: '/history',
          templateUrl: '/assets/features/services/templates/tabs/history.html',
          controller: 'ServicesRunsController',
          controllerAs: 'RunsController',
          ncyBreadcrumb: {
            parent: 'services.detail.runs',
            label: 'History'
          }
        });
  });
