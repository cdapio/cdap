/**
 * DashboardAddWdgtCtrl
 */

angular.module(PKG.name+'.feature.dashboard').controller('DashboardAddWdgtCtrl',
function ($scope, myDashboardsModel) {

   $scope.dashboard = myDashboardsModel.current();

});

