angular.module(PKG.name + '.commons')
  .directive('myAceEditor', function($window) {
    return {
      restrict: 'EA',
      scope: {
        model: '=ngModel'
      },
      template: '<div ui-ace="aceoptions" ng-model="model" aceEditorStyle></div>',
      controller: function($scope) {
        var config = $window.ace.require('ace/config');
        config.set('modePath', '/assets/bundle/ace-editor-worker-scripts/');
        $scope.aceoptions = {
          maxLines: 15,
          workerPath: '/assets/bundle/ace-editor-worker-scripts',
          mode: 'javascript',
          useWrapMode: false,
          newLineMode: 'unix'
        };
      }
    };
  });
