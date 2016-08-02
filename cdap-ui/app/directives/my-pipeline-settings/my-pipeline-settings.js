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
  .directive('myPipelineSettings', function(MyPipelineSettingsFactory, $compile) {
    return {
      restrict: 'A',
      scope: {
        store: '=',
        actionCreator: '=',
        isDisabled: '@',
        templateType: '@'
      },
      replace: false,
      link: function (scope, element) {
        var angularElement,
            widget;
        element.removeAttr('my-pipeline-settings');
        widget = MyPipelineSettingsFactory[scope.templateType];
        if (!widget) {
          return;
        }

        angularElement = angular.element(widget.element);
        angular.forEach(widget.attributes, function(value, key) {
          angularElement.attr(key, value);
        });

        var content = $compile(angularElement)(scope);
        element.append(content);
      }

    };
  });
