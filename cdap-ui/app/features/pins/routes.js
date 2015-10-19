angular.module(`${PKG.name}.feature.pins`)
  .config(function($stateProvider) {
    $stateProvider
      .state('pins', {
        url: '/pins',
        abstract: true,
        template: '<div ui-view></div>'
      })
        .state('pins.list', {
          url: '',
          templateUrl: '/assets/features/pins/templates/pins-list.html',
          controller: 'PinsListController',
          controllerAs: 'PinsListController',
          ncyBreadcrumb: {
            label: 'Pins'
          }
        });
  });
