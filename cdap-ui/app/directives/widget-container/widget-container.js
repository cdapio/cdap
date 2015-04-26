angular.module(PKG.name + '.commons')
  .directive('widgetContainer', function($compile) {

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
          case 'json-editor':
            angularElement = angular.element('<textarea class="form-control" cask-json-edit="model"></textarea>');
            angularElement.attr('placeholder', scope.myconfig[scope.name].info);
            element.append(angularElement);
            break;
          case 'javascript-editor':
            angularElement = angular.element('<div style="width: 400px;height: 300px;" ui-ace></div>');
            element.append(angularElement);
            break;
        }
        element.removeAttr('widget-container');
        $compile(element)(scope);
      }
    };

  });
