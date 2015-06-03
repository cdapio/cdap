angular.module(PKG.name + '.commons')
  .directive('myAceEditor', function($window, myHelpers) {
    return {
      restrict: 'EA',
      scope: {
        model: '=ngModel',
        config: '='
      },
      template: '<div ui-ace="aceoptions" ng-model="model" aceEditorStyle></div>',
      controller: function($scope) {
        var config = $window.ace.require('ace/config');
        config.set('modePath', '/assets/bundle/ace-editor-worker-scripts/');
        $scope.aceoptions = {
          workerPath: '/assets/bundle/ace-editor-worker-scripts',
          mode: 'javascript',
          useWrapMode: true,
          newLineMode: 'unix'
        };

      }
    };
  });
