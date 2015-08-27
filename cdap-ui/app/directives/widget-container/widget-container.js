angular.module(PKG.name + '.commons')
  .directive('widgetContainer', function($compile, $window, WidgetFactory) {
    return {
      restrict: 'A',
      scope: {
        name: '=',
        model: '=',
        myconfig: '=',
        properties: '=',
        widgetDisabled: '='
      },
      replace: false,
      link: function (scope, element) {
        var angularElement,
            widget,
            fieldset;
        if (WidgetFactory.registry[scope.myconfig.widget]) {
          widget = WidgetFactory.registry[scope.myconfig.widget];
        } else {
          widget = WidgetFactory.registry['__default__'];
        }

        fieldset = angular.element('<fieldset></fieldset>');
        fieldset.attr('ng-disabled', scope.widgetDisabled);

        angularElement = angular.element(widget.element);
        angular.forEach(widget.attributes, function(value, key) {
          angularElement.attr(key, value);
        });

        fieldset.append(angularElement);
        element.append(fieldset);

        element.removeAttr('widget-container');
        $compile(element)(scope);
      }
    };

  });
