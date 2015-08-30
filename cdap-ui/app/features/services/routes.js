angular.module(PKG.name + '.feature.services')
  .config(function($stateProvider, $urlRouterProvider, MYAUTH_ROLE) {
    $stateProvider
      .state('services', {
        url: '/services/:programId',
        abstract: true,
        parent: 'programs',
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'development'
        },
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
        template: '<ui-view/>'
      })
      .state('services.detail', {
        url: '/runs',
        templateUrl: '/assets/features/services/templates/detail.html',
        controller: 'ServicesRunsController',
        controllerAs: 'RunsController',
        ncyBreadcrumb: {
          parent: 'apps.detail.overview.status',
          label: '{{$state.params.programId}}'
        }
      })
        .state('services.detail.run', {
          url: '/:runid',
          templateUrl: '/assets/features/services/templates/tabs/runs/run-detail.html',
          controller: 'ServicesRunsDetailController',
          controllerAs: 'RunsDetailController',
          ncyBreadcrumb: {
            label: '{{$state.params.runid}}',
            parent: 'services.detail'
          }
        })
          .state('services.detail.run.makerequest', {
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
          });
  });
