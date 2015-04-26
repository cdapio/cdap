angular.module(PKG.name + '.feature.etlapps')
  .controller('PluginEditController', function($scope, MyDataSource, PluginConfigFactory) {
    $scope.configfetched = false;
    PluginConfigFactory.fetch($scope, $scope.$parent.metadata.type, $scope.plugin)
      .then(function(res) {
        $scope.configfetched = true;
        $scope.config = res;
      });
  });
