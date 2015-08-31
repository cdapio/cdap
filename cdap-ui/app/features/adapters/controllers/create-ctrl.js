angular.module(PKG.name + '.feature.adapters')
  .controller('AdapterCreateController', function(MyAppDAGService, $scope, rConfig, $modalStack, EventPipe, $window, $timeout) {

    var confirmOnPageExit = function (e) {

      if (!MyAppDAGService.isConfigTouched) { return; }
      // If we haven't been passed the event get the window.event
      e = e || $window.event;
      var message = 'You have unsaved changes.';
      // For IE6-8 and Firefox prior to version 4
      if (e) {
        e.returnValue = message;
      }
      // For Chrome, Safari, IE8+ and Opera 12+
      return message;
    };
    $window.onbeforeunload = confirmOnPageExit;

    $scope.$on('$stateChangeStart', function (event) {
      if (MyAppDAGService.isConfigTouched) {
        var response = confirm('You have unsaved changes. Are you sure you want to exit this page?');
        if (!response) {
          event.preventDefault();
        }
      }
    });

    if (rConfig) {
      $timeout(function() {
        MyAppDAGService.setNodesAndConnectionsFromDraft(rConfig);
      });
    }

    $scope.$on('$destroy', function() {
      $modalStack.dismissAll();
      $window.onbeforeunload = null;
      EventPipe.cancelEvent('plugin.reset');
      EventPipe.cancelEvent('schema.clear');
    });
  });
