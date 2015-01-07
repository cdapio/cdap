angular.module(PKG.name + '.commons')
  .directive('fileSelect', function($parse) {
    return {
      restrict: 'A',
      link: function(scope, element, attrs) {
        var fileElement = angular.element('<input style="width: 1px; height: 1px" type="file" multiple="true">');
        element.append(fileElement);
        element.bind('click', function(e) {
          fileElement[0].click();
        });
        var onFileSelect = $parse(attrs.onFileSelect);
        fileElement.bind('change', function(e) {
          onFileSelect(scope, {
            $files: e.target.files,
            $event: e
          })
        });

      }
    }
  })
