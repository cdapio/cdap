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
        templateUrl: '/assets/features/services/templates/detail.html',
        onEnter: function($state, $timeout) {

          $timeout(function() {
            if ($state.is('services.detail')) {
              $state.go('services.detail.status');
            }
          });

        },
        ncyBreadcrumb: {
          parent: 'apps.detail.overview',
          label: '{{$state.params.programId | caskCapitalizeFilter}}'
        }
      })
        .state('services.detail.status', {
          url: '/status',
          templateUrl: '/assets/features/services/templates/tabs/status.html',
          ncyBreadcrumb: {
            skip: true
          }
        })
          .state('services.detail.status.makerequest', {
            params: {
              requestUrl: null,
              requestMethod: null
            },
            onEnter: function ($state, $modal) {
              var modal = $modal({
                template: '/assets/features/services/templates/tabs/status/make-request.html',
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
            skip: true
          }
        })
        .state('services.detail.logs', {
          url: '/logs?filter',
          reloadOnSearch: false,
          controller: 'ServicesLogsController',
          template: '<my-log-viewer data-model=\'logs\'></my-log-viewer>',
          ncyBreadcrumb: {
            skip: true
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
