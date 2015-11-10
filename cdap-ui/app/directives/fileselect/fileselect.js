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
  .directive('myFileSelect', function($parse) {
    return {
      restrict: 'E',
      scope: true,
      templateUrl: 'fileselect/fileselect.html',
      link: function(scope, element, attrs) {
        // Enabling Customizability.
        scope.buttonLabel = attrs.buttonLabel || 'Upload';
        scope.buttonIcon = attrs.buttonIcon || 'fa-plus';
        scope.buttonClass = attrs.buttonClass || '';
        scope.buttonDisabled = attrs.buttonDisabled === 'true';

        attrs.$observe('buttonDisabled', function () {
          scope.buttonDisabled = attrs.buttonDisabled === 'true';
        });

        if (attrs.dropdown === undefined) {
          attrs.dropdown = false;
        }

        scope.isDropdown = attrs.dropdown;

        var fileElement = angular.element('<input class="sr-only" type="file" multiple="true">');
        element.append(fileElement);
        element.bind('click', function() {
          fileElement[0].click();
        });

        var onFileSelect = $parse(attrs.onFileSelect);
        fileElement.bind('change', function(e) {
          onFileSelect(scope, {
            $files: e.target.files
          });
          // If upload fails and if the same file is uploaded again (fixed file)
          // the onchange will not be triggered. This is to enable that.
          this.value = null;
        });

      }
    };
  });
