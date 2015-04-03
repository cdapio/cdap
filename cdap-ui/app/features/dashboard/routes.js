angular.module(PKG.name+'.feature.dashboard')
  .config(function ($stateProvider, $urlRouterProvider, MYAUTH_ROLE) {

    var path = '/assets/features/operation28/';
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
        templateUrl: '/assets/features/dashboard/dashboard.html',
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
          templateUrl: path + 'tab/charts.html',
          controller: 'Op28CdapCtrl'
        })

        .state('dashboard.standard.system', {
          url: '/system',
          templateUrl: path + 'tab/charts.html',
          controller: 'Op28SystemCtrl'
        })

        .state('dashboard.standard.apps', {
          url: '/apps',
          templateUrl: path + 'tab/apps.html',
          controller: 'Op28AppsCtrl'
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


      // .state('dashboard', {
      //   data: {
      //     authorizedRoles: MYAUTH_ROLE.all,
      //     highlightTab: 'operations'
      //   },
      //   parent: 'ns',
      //   url: '/dashboard/:tab',
      //   templateUrl: '/assets/features/dashboard/main.html',
      //   controller: 'DashboardCtrl',
      //   resolve: {
      //     rDashboardsModel: function ($stateParams, MyDashboardsModel) {
      //       return (new MyDashboardsModel($stateParams.namespace)).$promise;
      //     }
      //
      //   }
      // })

        //
        // .state('dashboard.cdap', {
        //   url: '/cdap',
        //   templateUrl: path + 'tab/charts.html',
        //   controller: 'Op28CdapCtrl'
        // })
        // .state('operations.system', {
        //   url: '/system',
        //   templateUrl: path + 'tab/charts.html',
        //   controller: 'Op28SystemCtrl'
        // })
        // .state('operations.apps', {
        //   url: '/apps',
        //   templateUrl: path + 'tab/apps.html',
        //   controller: 'Op28AppsCtrl'
        // })

        .state('dashboard.user.addwdgt', {
          url: '/widget/add',
          onEnter: function ($state, $bootstrapModal, $rootScope, rDashboardsModel, tab) {
            var scope = $rootScope.$new();
            scope.currentDashboard = rDashboardsModel.current();
            $bootstrapModal.open({
              templateUrl: '/assets/features/dashboard/partials/addwdgt.html',
              size: 'md',
              backdrop: true,
              keyboard: true,
              scope: scope,
              controller: 'DashboardAddWdgtCtrl'
            }).result.finally(function() {
              $state.go('dashboard.user', {tab: tab}, { reload: true });
            });
          }
        })

      ;


  });
