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
        var angularElement,
            infoElement;
        switch(scope.myconfig.widget) {
          case 'textbox':
            angularElement = angular.element('<input class="form-control" ng-model="model"/>');
            angularElement.attr('placeholder', scope.myconfig.description);
            break;
          case 'password':
            angularElement = angular.element('<input class="form-control" ng-model="model"/>');
            angularElement.attr('type', 'password');
            angularElement.attr('placeholder', scope.myconfig.info);
            break;
          case 'json-editor':
            angularElement = angular.element('<textarea style="resize: vertical;" class="form-control" cask-json-edit="model"></textarea>');
            angularElement.attr('placeholder', scope.myconfig.info);
            break;
          case 'javascript-editor':
            angularElement = angular.element('<div style="width: 400px;height: 300px;" ui-ace></div>');
            break;
          case 'default':
            angularElement = angular.element('<input ng-model="model"/ >');
            break;
        }

        element.append(angularElement);
        if (scope.myconfig.info) {
          infoElement = angular.element('<span class="fa fa-exclamation-circle text-info" tooltip="{{myconfig.info}}"></span>');
          element.append(infoElement);
        }
        element.removeAttr('widget-container');
        $compile(element)(scope);
      }
    };

  });
