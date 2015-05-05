angular.module(PKG.name+'.feature.dashboard')
  .config(function ($stateProvider, $urlRouterProvider, MYAUTH_ROLE) {

    /**
     * State Configurations
     */
    $stateProvider

      .state('dashboard', {
        url: '/dashboard',
        parent: 'ns',
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'dashboard'
        },
        templateUrl: '/assets/features/dashboard/templates/dashboard.html',
        resolve: {
          rDashboardsModel: function ($stateParams, MyDashboardsModel) {
            return (new MyDashboardsModel($stateParams.namespace)).$promise;
          }

        },
        controller: 'DashboardCtrl'
      })

      .state('dashboard.standard', {
        url: '/standard',
        abstract: true,
        template: '<ui-view/>'
      })

      .state('dashboard.standard.cdap', {
        url: '/cdap',
        templateUrl: '/assets/features/dashboard/templates/staticdashboard.html',
        controller: 'OpsCdapCtrl'
      })

      .state('dashboard.user', {
        url: '/user/:tab',
        templateUrl: '/assets/features/dashboard/templates/userdashboard.html',
        controller: 'DashboardCtrl',
        resolve: {
          tab: function($stateParams) {
            return $stateParams.tab;
          }
        }
      })

      .state('dashboard.user.addwdgt', {
        url: '/widget/add',
        onEnter: function ($state, $bootstrapModal, $rootScope, rDashboardsModel, tab) {
          var scope = $rootScope.$new();
          scope.currentBoard = rDashboardsModel.current();
          $bootstrapModal.open({
            templateUrl: '/assets/features/dashboard/templates/partials/addwdgt.html',
            size: 'md',
            backdrop: true,
            keyboard: true,
            scope: scope,
            controller: 'DashboardAddWdgtCtrl'
          }).result.finally(function() {
            $state.go('dashboard.user', {tab: tab}, { reload: true });
          });
        },
        onExit: function($modalStack) {
          $modalStack.dismissAll();
        }
      })

      ;


  });
