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
      .state('services.list', {
        url: '/list',
        templateUrl: '/assets/features/services/templates/list.html',
        controller: 'ServicesListController',
        ncyBreadcrumb: {
          parent: 'apps.detail.overview',
          label: 'Services'
        }
      })
      .state('services.detail', {
        url: '/:programId',
        controller: 'ServicesDetailController',
        templateUrl: '/assets/features/services/templates/detail.html',
        resolve : {
          rRuns: function(MyDataSource, $stateParams, $q) {
            var defer = $q.defer(),
                dataSrc = new MyDataSource();

            dataSrc.request({
              _cdapPath: '/namespaces/' + $stateParams.namespace +
                          '/apps/' + $stateParams.appId +
                          '/services/' + $stateParams.programId +
                          '/runs'
            })
            .then(function (res) {
              defer.resolve(res);
            });

            return defer.promise;

          }
        },
        ncyBreadcrumb: {
          parent: 'apps.detail.overview',
          label: 'Services',
          skip: true
        }
      })
        .state('services.detail.runs', {
          url: '/runs',
          templateUrl: '/assets/features/services/templates/tabs/runs.html',
          controller: 'ServicesRunsController',
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


        .state('services.detail.data', {
          url: '/data',
          templateUrl: '/assets/features/services/templates/tabs/data.html',
          ncyBreadcrumb: {
            skip: true
          }
        })
        .state('services.detail.metadata', {
          url: '/metadata',
          templateUrl: '/assets/features/services/templates/tabs/metadata.html',
          ncyBreadcrumb: {
            skip: true
          }
        })
        .state('services.detail.history', {
          url: '/history',
          templateUrl: '/assets/features/services/templates/tabs/history.html',
          ncyBreadcrumb: {
            parent: 'apps.detail.overview',
            label: '{{$state.params.programId}} / History'
          }
        })

        .state('services.detail.resources', {
          url: '/resource',
          templateUrl: '/assets/features/services/templates/tabs/resources.html',
          ncyBreadcrumb: {
            skip: true
          }
        });
  });
