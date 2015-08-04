angular.module(PKG.name + '.commons')
  .directive('myMaxlength', function () {
    return {
      require: 'ngModel',
      link: function (scope, elem, attrs, ctrl) {
        var maxlength = Number(attrs.myMaxlength);

        function input(text) {
          if (text.length > maxlength) {
            var transform = text.substring(0, maxlength);

            ctrl.$setViewValue(transform);
            ctrl.$render();

            return transform;
          }

          return text;
        }

        ctrl.$parsers.push(input);

      }
    };
  });
