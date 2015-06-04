angular.module(PKG.name + '.commons')
  .directive('widgetContainer', function($compile, $window, WidgetFactory) {
    return {
      restrict: 'A',
      scope: {
        name: '=',
        model: '=',
        myconfig: '=',
        properties: '='
      },
      replace: false,
      link: function (scope, element) {
        var angularElement,
            widget;
        if (WidgetFactory.registry[scope.myconfig.widget]) {
          widget = WidgetFactory.registry[scope.myconfig.widget];
        } else {
          widget = WidgetFactory.registry['__default__'];
        }
        angularElement = angular.element(widget.element);
        angular.forEach(widget.attributes, function(value, key) {
          angularElement.attr(key, value);
        });
        element.append(angularElement);

        element.removeAttr('widget-container');
        $compile(element)(scope);
      }
    };

  });
