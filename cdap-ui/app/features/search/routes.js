angular.module(`${PKG.name}.feature.search`)
  .config(function($stateProvider) {
    $stateProvider
      .state('search', {
        parent: 'ns',
        url: '/search',
        templateUrl: '/assets/features/search/templates/search.html',
        controller: 'SearchController',
        controllerAs: 'SearchController'
      })
        .state('search.objectswithtags', {
          url: '/search/:tag',
          templateUrl: '/assets/features/search/templates/search-objectwithtags.html',
          controller: 'SearchObjectWithTagsController',
          controllerAs: 'SearchObjectWithTagsController'
        });
  });
