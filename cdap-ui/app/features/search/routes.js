angular.module(`${PKG.name}.feature.search`)
  .config(function($stateProvider) {
    $stateProvider
      .state('search', {
        parent: 'ns',
        url: '/search',
        abstract: true,
        templateUrl: '/assets/features/search/templates/search.html'
      })
        .state('search.list', {
          url: '',
          templateUrl: '/assets/features/search/templates/search-list-view.html',
          controller: 'SearchController',
          controllerAs: 'SearchController',
          ncyBreadcrumb: {
            label: 'Search'
          }
        })
        .state('search.objectswithtags', {
          url: '/:tag',
          templateUrl: '/assets/features/search/templates/search-objects-with-tags.html',
          controller: 'SearchObjectWithTagsController',
          controllerAs: 'SearchObjectWithTagsController',
          ncyBreadcrumb: {
            label: '{{$state.params.tag}}',
            parent: 'search.list'
          }
        });
  });
