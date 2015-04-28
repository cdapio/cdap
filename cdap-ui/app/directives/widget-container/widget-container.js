angular.module(PKG.name + '.commons')
  .directive('widgetContainer', function($compile, $window, WidgetFactory) {
    return {
      restrict: 'A',
      scope: {
        name: '=',
        model: '=',
        myconfig: '='
      },
      replace: false,
      link: function (scope, element, attrs) {
        var angularElement,
            infoElement,
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

        if (scope.myconfig.info) {
          infoElement = angular.element('<a href="#"><i class="fa fa-exclamation-circle text-info" tooltip="{{myconfig.info}}"></i></a>');
          element.append(infoElement);
        }
        element.removeAttr('widget-container');
        $compile(element)(scope);
      }
    };

  });
