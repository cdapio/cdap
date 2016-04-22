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
  .directive('pluginFunctions', function($compile, $window, PluginsFunctionsFactory) {
    return {
      restrict: 'E',
      scope: {
        fnConfig: '=',
        node: '=',
        isDisabled: '='
      },
      replace: false,
      link: function (scope, element) {

        if (!scope.fnConfig) { return; }
        var fn = PluginsFunctionsFactory.registry[scope.fnConfig.widget];

        var fnElem = angular.element(fn.element);

        angular.forEach(fn.attributes, function(value, key) {
          fnElem.attr(key, value);
        });
        element.append(fnElem);
        $compile(element)(scope);
      }
    };
  });
