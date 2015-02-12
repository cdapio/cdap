angular.module(PKG.name+'.feature.dashboard')
  .config(function ($stateProvider, $urlRouterProvider, MYAUTH_ROLE) {

    // $urlRouterProvider
    //   .when('/operations', '/operations/whatever');

    var path = '/assets/features/operation28/';

    /**
     * State Configurations
     */
    $stateProvider

      .state('operations', {
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'operations'
        },
        url: '/operations',
        templateUrl: path + 'ops.html',
        controller: 'OperationsCtrl'
      })
        .state('operations.cdap', {
          url: '/cdap',
          templateUrl: path + 'tab/cdap.html'
        })
        .state('operations.system', {
          url: '/system',
          templateUrl: path + 'tab/system.html'
        })
        .state('operations.apps', {
          url: '/apps',
          templateUrl: path + 'tab/apps.html'
        })
      ;


  });
