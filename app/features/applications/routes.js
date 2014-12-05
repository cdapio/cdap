angular.module(PKG.name+'.feature.applications')
  .config(function ($stateProvider, $urlRouterProvider) {


    /**
     * Redirects and Otherwise
     */
    $urlRouterProvider
      .otherwise(function($injector, $location){
        $injector.get('$state').go($location.path() ? '404' : 'applications');
      });


    /**
     * State Configurations
     */
    $stateProvider

      .state('applications', {
        url: '/ns/:namespace/apps',
        templateUrl: '/assets/features/applications/applications.html',
        controller: 'ApplicationController'
      })

      .state('applications.status', {
        url: '/ns/:namespace/apps/:app/status',
        templateUrl: '/assets/features/applications/templates/status.html',
        controller: 'ApplicationController'
      })

      .state('applications.data', {
        url: '/ns/:namespace/apps/:app/data',
        templateUrl: '/assets/features/applications/templates/data.html',
        controller: 'ApplicationController'
      })

      .state('applications.history', {
        url: '/ns/:namespace/apps/:app/history',
        templateUrl: '/assets/features/applications/templates/history.html',
        controller: 'ApplicationController'
      })

      .state('applications.metadata', {
        url: '/ns/:namespace/apps/:app/metadata',
        templateUrl: '/assets/features/applications/templates/metadata.html',
        controller: 'ApplicationController'
      })

      .state('applications.resources', {
        url: '/ns/:namespace/apps/:app/resources',
        templateUrl: '/assets/features/applications/templates/resources.html',
        controller: 'ApplicationController'
      })

      .state('applications.schedules', {
        url: '/ns/:namespace/apps/:app/schedules',
        templateUrl: '/assets/features/applications/templates/schedules.html',
        controller: 'ApplicationController'
      })


  });
