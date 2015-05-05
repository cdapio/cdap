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
          newLineMode: 'unix',
          onLoad: aceLoaded
        };

        function aceLoaded(editor) {
          togglePlaceHolder();

          function togglePlaceHolder() {
            var shouldShow = !editor.session.getValue().length;
            var node = editor.renderer.emptyMessageNode;
            if (!shouldShow && node) {
                editor.renderer.scroller.removeChild(editor.renderer.emptyMessageNode);
                editor.renderer.emptyMessageNode = null;
            } else if (shouldShow && !node) {
                node = editor.renderer.emptyMessageNode = document.createElement("div");
                node.textContent = myHelpers.objectQuery($scope, 'config', 'properties', 'default');
                node.className = "ace_invisible ace_emptyMessage";
                node.style.padding = "0 5px";
                editor.renderer.scroller.appendChild(node);
            }
          }

          editor.on("input", togglePlaceHolder);
        }
      }
    };
  });
