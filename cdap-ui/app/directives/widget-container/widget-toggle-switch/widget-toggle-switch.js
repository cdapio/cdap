/*
 * Copyright © 2017-2018 Cask Data, Inc.
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

 const CHAR_LIMIT = 64;

angular.module(PKG.name + '.commons')
  .directive('myToggleSwitch', function() {
    return {
      restrict: 'E',
      scope: {
        model: '=ngModel',
        config: '=',
        disabled: '=',
      },
      templateUrl: 'widget-container/widget-toggle-switch/widget-toggle-switch.html',
      controller: function($scope, myHelpers) {
        const onValue = myHelpers.objectQuery($scope.config, 'widget-attributes', 'on', 'value') || 'on';
        const offValue = myHelpers.objectQuery($scope.config, 'widget-attributes', 'off', 'value') || 'off';
        const defaultValue = myHelpers.objectQuery($scope.config, 'widget-attributes', 'default') || onValue;
        const onLabel = myHelpers.objectQuery($scope.config, 'widget-attributes', 'on', 'label') || 'On';
        const offLabel = myHelpers.objectQuery($scope.config, 'widget-attributes', 'off', 'label') || 'Off';

        $scope.onLabel = onLabel.slice(0, CHAR_LIMIT);
        $scope.offLabel = offLabel.slice(0, CHAR_LIMIT);
        $scope.model = $scope.model || defaultValue;
        $scope.isOn = $scope.model === onValue;
        $scope.onToggle = () => ($scope.isOn = !$scope.isOn);

        $scope.$watch('isOn', () => {
          $scope.model = $scope.isOn ? onValue : offValue;
        });
      }
    };
  });
