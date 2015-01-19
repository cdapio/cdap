angular.module(PKG.name + '.commons')
  .directive('myFileSelect', function($parse) {
    return {
      restrict: 'E',
      scope: true,
      templateUrl: 'fileselect/fileselect.html',
      link: function(scope, element, attrs) {
        // Enabling Customizability.
        scope.buttonLabel = attrs.buttonLabel || 'Upload';
        scope.buttonIcon = attrs.buttonIcon || 'fa-upload';
        scope.buttonSize = attrs.buttonSize || '';
        scope.buttonDisabled = !!attrs.buttonDisabled || false;

        var fileElement = angular.element('<input class="sr-only" type="file" multiple="true">');
        element.append(fileElement);
        element.bind('click', function(e) {
          fileElement[0].click();
        });

        var onFileSelect = $parse(attrs.onFileSelect);
        fileElement.bind('change', function(e) {
          onFileSelect(scope, {
            $files: e.target.files
          });
          // If upload fails and if the same file is uploaded again (fixed file)
          // the onchange will not be triggered. This is to enable that.
          this.value = null;
        });

      }
    };
  });
