angular.module(PKG.name + '.feature.adapters')
  .controller('PluginEditController', function($scope, MyDataSource, PluginConfigFactory) {
    $scope.configfetched = false;
    $scope.noconfig = false;
    $scope.noproperty = Object.keys(
      $scope.plugin.properties || {}
    ).length;

    if ($scope.noproperty) {
      PluginConfigFactory.fetch($scope, $scope.$parent.metadata.type, $scope.plugin.name)
        .then(
          function success(res) {
            $scope.configfetched = true;
            $scope.config = res;
          },
          function error(err) {
            $scope.noconfig = true;
          }
        );
    }
  });
