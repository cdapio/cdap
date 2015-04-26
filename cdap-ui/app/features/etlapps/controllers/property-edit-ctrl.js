angular.module(PKG.name + '.feature.etlapps')
  .controller('SourceEditController', function($scope, MyDataSource, PluginConfigFactory) {
    $scope.configfetched = false;
    PluginConfigFactory.fetch($scope, 'etlRealtime', 'TwitterSource')
      .then(function(res) {
        $scope.configfetched = true;
        $scope.config = res;
      });
  })
  .controller('SinkEditController', function($scope, MyDataSource, PluginConfigFactory) {
    $scope.configfetched = false;
    PluginConfigFactory.fetch($scope, 'etlRealtime', 'StreamSink')
      .then(function(res) {
        $scope.configfetched = true;
        $scope.config = res;
      });
  });
