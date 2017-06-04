/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
*/
angular.module(PKG.name + '.commons')
.directive('myPipelineResourceFactory', function(MyPipelineResourceFactory, $compile) {
  return {
    restrict: 'A',
    replace: false,
    scope: {
      store: '=',
      actionCreator: '=',
      isDisabled: '=',
      resourceType: '@',
      onMemoryChange: '&',
      onCoreChange: '&',
      memoryMbValue: '=',
      virtualCoresValue: '='
    },
    link: function(scope, element) {
      var angularElement,
          widget,
          divElement;
      element.removeAttr('my-pipeline-resource-factory');
      widget = MyPipelineResourceFactory[scope.resourceType];
      if (!widget) {
        return;
      }

      divElement = angular.element('<div></div>');
      angularElement = angular.element(widget.element);
      angular.forEach(widget.attributes, function(value, key) {
        angularElement.attr(key, value);
      });

      divElement.append(angularElement);
      var content = $compile(divElement)(scope);
      element.append(content);
    }
  };
})
.directive('myPipelineDriverResource', function() {
  return {
    restrict: 'E',
    scope: {
      actionCreator: '=',
      store: '=',
      isDisabled: '=',
      onMemoryChange: '&',
      onCoreChange: '&',
      memoryMbValue: '=',
      virtualCoresValue: '='
    },
    templateUrl: 'my-pipeline-resource/my-pipeline-resource.html',
    controller: 'MyPipelineDriverResourceCtrl'
  };
})
.directive('myPipelineExecutorResource', function() {
  return {
    restrict: 'E',
    scope: {
      actionCreator: '=',
      store: '=',
      isDisabled: '=',
      onMemoryChange: '&',
      onCoreChange: '&',
      memoryMbValue: '=',
      virtualCoresValue: '='
    },
    templateUrl: 'my-pipeline-resource/my-pipeline-resource.html',
    controller: 'MyPipelineExecutorResourceCtrl'
  };
})
.directive('myPipelineClientResource', function() {
  return {
    restrict: 'E',
    scope: {
      actionCreator: '=',
      store: '=',
      isDisabled: '=',
      onMemoryChange: '&',
      onCoreChange: '&',
      memoryMbValue: '=',
      virtualCoresValue: '='
    },
    templateUrl: 'my-pipeline-resource/my-pipeline-resource.html',
    controller: 'MyPipelineClientResourceCtrl'
  };
});
