angular.module(PKG.name + '.feature.adapters')
  .controller('CanvasController', function (MyPlumbService, $scope, $modalStack) {

    this.nodes = [];
    
    function errorNotification(errors) {
      angular.forEach(this.pluginTypes, function (type) {
        delete type.error;
        if (errors[type.name]) {
          type.error = errors[type.name];
        }
      });
    }

    MyPlumbService.errorCallback(errorNotification.bind(this));

    $scope.$on('$destroy', function() {
      $modalStack.dismissAll();
    });

  });
