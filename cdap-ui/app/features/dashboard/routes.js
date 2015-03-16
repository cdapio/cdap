angular.module(PKG.name+'.feature.dashboard')
  .config(function ($stateProvider, $urlRouterProvider, MYAUTH_ROLE) {

    /**
     * State Configurations
     */
    $stateProvider

      .state('dashboard', {
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'operations'
        },
        parent: 'ns',
        url: '/dashboard/:tab',
        templateUrl: '/assets/features/dashboard/main.html',
        controller: 'DashboardCtrl',
        resolve: {
          rDashboardsModel: function ($stateParams, MyDashboardsModel) {
            return (new MyDashboardsModel($stateParams.namespace)).$promise;
          }

        }
      })

        .state('dashboard.addwdgt', {
          // url: '/widget/add',
          onEnter: function ($state, $modal, $rootScope, rDashboardsModel) {
            var scope = $rootScope.$new();
            scope.currentDashboard = rDashboardsModel.current();
            $modal({
              template: '/assets/features/dashboard/partials/addwdgt.html',
              scope: scope
            }).$promise.then(function () {
              $state.go('^', $state.params);
            });
          }
        })

      ;


  });
