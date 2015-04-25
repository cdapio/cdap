angular.module(PKG.name + '.feature.etlapps')
  .controller('SourceEditController', function($scope, MyDataSource, PluginConfigFactory) {
    $scope.somethingfetched = false;
    PluginConfigFactory.fetch($scope, 'etlRealtime', 'TwitterSource')
      .then(function(res) {
        var something = {};
         var combined = angular.extend({}, res.groups.group1.fields, res.groups.group2.fields);
         angular.forEach(combined, function(value, key) {
           something[key] = {
             description: value['widget-description'],
             info: value['widget-info'],
             label: value['widget-label'],
             properties: value['widget-properties'],
             type: value['widget-type']
           };
         });
         $scope.somethingfetched = true;
         $scope.something = something;
         console.info("Something:", something);
      });
  });
