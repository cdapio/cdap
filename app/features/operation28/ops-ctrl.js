/**
 * OperationsCtrl
 */

angular.module(PKG.name+'.feature.operation28')
.controller('OperationsCtrl',
function ($scope, $state) {

  if($state.is('operations')) {
    $state.go('operations.cdap');
  }



});

