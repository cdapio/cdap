angular.module(PKG.name + '.feature.spark')
  .controller('SparkDetailController', function($scope, MyDataSource, $state, MY_CONFIG) {
    $scope.isEnterprise = MY_CONFIG.isEnterprise;
  });
