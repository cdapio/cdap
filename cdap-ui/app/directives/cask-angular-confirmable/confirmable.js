/*
 * Copyright © 2015-2018 Cask Data, Inc.
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

/**
 * caskConfirmable
 *
 * adds a "caskConfirm" method on the scope. call that, and
 *  the expression in "cask-confirmable" attribute will be evaluated
 *  after the user accepts the confirmation dialog. Eg:
 *
 * <a ng-click="caskConfirm()"
 *       cask-confirmable="doDelete(model)"
 *       data-confirmable-title="Hold on..."
 *       data-confirmable-content="Are you absolutely sure?"
 * >delete</a>
 */

angular.module(PKG.name+'.commons').directive('caskConfirmable',
function caskConfirmableDirective ($modal, $sce) {
  return {
    restrict: 'A',
    link: function (scope, element, attrs) {

      scope.caskConfirm = function () {

        var modal, modalScope;

        modalScope = scope.$new(true);

        modalScope.customClass = attrs.confirmableModalClass || '';
        modalScope.doConfirm = function() {
          modal.hide();
          scope.$eval(attrs.caskConfirmable);
        };
        var confirmableContent = $sce.getTrustedHtml(attrs.confirmableContent);
        modal = $modal({
          scope: modalScope,
          template: 'cask-angular-confirmable/confirm-modal.html',
          title: attrs.confirmableTitle || 'Confirmation',
          content: confirmableContent || 'Are you sure?',
          placement: 'center',
          show: true
        });

      };

    }
  };

});
