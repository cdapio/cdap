/*
 * Copyright © 2015 Cask Data, Inc.
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
  .directive('myAceEditor', function($window) {
    return {
      restrict: 'EA',
      scope: {
        model: '=ngModel',
        config: '=',
        mode: '@'
      },
      template: '<div ui-ace="aceoptions" ng-model="model" aceEditorStyle></div>',
      controller: function($scope) {
        var config = $window.ace.require('ace/config');
        config.set('modePath', '/assets/bundle/ace-editor-worker-scripts/');
        $scope.aceoptions = {
          workerPath: '/assets/bundle/ace-editor-worker-scripts',
          mode: $scope.mode || 'javascript',
          useWrapMode: true,
          newLineMode: 'unix',
          advanced: {
            tabSize: 2
          }
        };

      }
    };
  });
