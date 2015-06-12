angular.module(PKG.name + '.feature.explore')
  .config(function($stateProvider, $urlRouterProvider, MYAUTH_ROLE) {
    $stateProvider
      .state('explore', {
        url: '/explore',
         data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'development'
        },
        templateUrl: '/assets/features/explore/explore.html',
        controller: 'GlobalExploreController',
        controllerAs: 'GlobalExploreController',
        parent: 'ns',
        ncyBreadcrumb: {
          label: 'Explore',
          parent: 'overview'
        }
      });
  });
