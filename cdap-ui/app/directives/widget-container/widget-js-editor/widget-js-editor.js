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

const LINE_HEIGHT = 20; // roughly line height used in plugin modal
const DEFAULT_TEXTAREA_HEIGHT = 100;
const DEFAULT_CODE_EDITOR_HEIGHT = 500;

angular.module(PKG.name + '.commons')
  .directive('myAceEditor', function($window) {
    return {
      restrict: 'EA',
      scope: {
        model: '=ngModel',
        config: '=',
        mode: '@',
        disabled: '=',
        rows: '@',
      },
      template: `<div ui-ace="aceoptions"
                ng-model="model"
                ng-readonly="disabled"
                ng-class="{'form-control': mode === 'plain_text'}"
                ng-style="editorStyle"></div>`,
      controller: function($scope) {
        var config = $window.ace.require('ace/config');
        config.set('modePath', '/assets/bundle/ace-editor-worker-scripts/');
        $scope.aceoptions = {
          workerPath: '/assets/bundle/ace-editor-worker-scripts',
          mode: $scope.mode || 'javascript',
          useWrapMode: true,
          newLineMode: 'unix',
          advanced: {
            tabSize: 2,
          }
        };
        let height;
        // textarea widget
        if ($scope.mode && ['plain_text', 'sql'].indexOf($scope.mode) !== -1) {
          height = DEFAULT_TEXTAREA_HEIGHT;
          if ($scope.rows && $scope.rows > 0) {
            height = $scope.rows * LINE_HEIGHT;
          }
        } else {
          // default height for other code editors (e.g. Python, Javascript)
          height = DEFAULT_CODE_EDITOR_HEIGHT;
        }
        $scope.editorStyle = { height: height + 'px' };
      }
    };
  });
