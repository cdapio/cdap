angular.module(PKG.name + '.feature.datasets')
  .config(function($stateProvider, $urlRouterProvider, MYAUTH_ROLE) {
    $stateProvider
      .state('datasets', {
        abstract: true,
        template: '<ui-view/>',
        url: '/datasets',
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'development'
        },
        parent: 'ns'
      })

      .state('datasets.list', {
        url: '',
        templateUrl: '/assets/features/datasets/templates/list.html',
        controller: 'CdapDatasetsListController',
        ncyBreadcrumb: {
          label: 'Datasets',
          parent: 'data.list'
        }
      })

      .state('datasets.detail', {
        url: '/:datasetId',
        abstract: true,
        resolve: {
          explorableDatasets: function explorableDatasets(myExploreApi, $stateParams, $q, $filter) {
            var params = {
              namespace: $stateParams.namespace
            };
            var defer = $q.defer(),
                filterFilter = $filter('filter');

            // Checking whether dataset is explorable
            myExploreApi.list(params)
              .$promise
              .then(function (res) {
                var match = filterFilter(res, $stateParams.datasetId);
                if (match.length === 0) {
                  defer.resolve(false);
                } else {
                  defer.resolve(true);
                }
              });
            return defer.promise;
          }
        },
        template: '<ui-view/>'
      })
        .state('datasets.detail.overview', {
          url: '/overview',
          templateUrl: '/assets/features/datasets/templates/detail.html',
          controller: 'CdapDatasetsDetailController',
          ncyBreadcrumb: {
            skip: true
          }
        })

          .state('datasets.detail.overview.status', {
            url: '/status',
            templateUrl: '/assets/features/datasets/templates/tabs/status.html',
            controller: 'CdapDatasetDetailStatusController',
            ncyBreadcrumb: {
              parent: 'data.list',
              label: '{{$state.params.datasetId}}'
            }
          })

          .state('datasets.detail.overview.explore', {
            url: '/explore',
            templateUrl: '/assets/features/datasets/templates/tabs/explore.html',
            ncyBreadcrumb: {
              label: 'Explore',
              parent: 'datasets.detail.overview.status'
            }
          })

          .state('datasets.detail.overview.programs', {
            url: '/programs',
            templateUrl: '/assets/features/datasets/templates/tabs/programs.html',
            ncyBreadcrumb: {
              label: 'Programs',
              parent: 'datasets.detail.overview.status'
            }
          })

          .state('datasets.detail.overview.metadata', {
            url: '/metadata',
            templateUrl: '/assets/features/datasets/templates/tabs/metadata.html',
            ncyBreadcrumb: {
              label: 'Metadata',
              parent: 'datasets.detail.overview.status'
            }
          });
  });
