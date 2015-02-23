angular.module(PKG.name+'.feature.operation28')
  .config(function ($stateProvider, $urlRouterProvider, MYAUTH_ROLE) {

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
        parent: 'ns',
        url: '/operations',
        templateUrl: path + 'ops.html',
        controller: function ($scope, $state, MY_CONFIG) {
          $scope.$on('$stateChangeSuccess', function(){
            if($state.is('operations')) {
              $state.go('operations.cdap');
            }
          });
          $scope.isEnterprise = MY_CONFIG.isEnterprise;
        }
      })
        .state('operations.cdap', {
          url: '/cdap',
          templateUrl: path + 'tab/charts.html',
          controller: 'Op28CdapCtrl'
        })
        .state('operations.system', {
          url: '/system',
          templateUrl: path + 'tab/charts.html',
          controller: 'Op28SystemCtrl'
        })
        .state('operations.apps', {
          url: '/apps',
          templateUrl: path + 'tab/apps.html',
          controller: 'Op28AppsCtrl'
        })
      ;


  });
