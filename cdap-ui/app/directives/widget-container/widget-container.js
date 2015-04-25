angular.module(PKG.name + '.commons')
  .directive('widgetContainer', function($compile) {
    var registry = {
      'textbox': 'textbox'
    };

    return {
      restrict: 'A',
      scope: {
        name: '=',
        model: '=',
        myconfig: '='
      },
      replace: false,
      link: function (scope, element, attrs) {
        var angularElement;
        switch(scope.myconfig[scope.name].type) {
          case 'textbox':
            angularElement = angular.element('<input class="form-control" ng-model="model"/>');
            angularElement.attr('placeholder', scope.myconfig[scope.name].info);
            element.append(angularElement);
            break;
          case 'password':
            angularElement = angular.element('<input class="form-control" ng-model="model"/>');
            angularElement.attr('type', 'password');
            angularElement.attr('placeholder', scope.myconfig[scope.name].info);
            element.append(angularElement);
            break;
          case 'textarea':
            angularElement = angular.element('<textarea class="form-control" ng-model="model"></textarea>');
            angularElement.attr('placeholder', scope.myconfig[scope.name].info);
            element.append(angularElement);
            break;
        }
        element.removeAttr('widget-container');
        $compile(element)(scope);
      }
    };

  });
