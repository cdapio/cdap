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

angular.module(PKG.name + '.feature.adapters')
  .controller('AdapterCreateStudioController', function(MyAppDAGService, $scope, rConfig, $modalStack, EventPipe, $window, $timeout, MyConsoleTabService) {
    this.isExpanded = true;
    var confirmOnPageExit = function (e) {

      if (!MyAppDAGService.isConfigTouched) { return; }
      // If we haven't been passed the event get the window.event
      e = e || $window.event;
      var message = 'You have unsaved changes.';
      // For IE6-8 and Firefox prior to version 4
      if (e) {
        e.returnValue = message;
      }
      // For Chrome, Safari, IE8+ and Opera 12+
      return message;
    };
    $window.onbeforeunload = confirmOnPageExit;

    $scope.$on('$stateChangeStart', function (event) {
      if (MyAppDAGService.isConfigTouched) {
        var response = confirm('You have unsaved changes. Are you sure you want to exit this page?');
        if (!response) {
          event.preventDefault();
        }
      }
    });

    if (rConfig) {
      $timeout(function() {
        MyAppDAGService.setNodesAndConnectionsFromDraft(rConfig);
      });
    }

    $scope.$on('$destroy', function() {
      $modalStack.dismissAll();
      MyConsoleTabService.resetMessages();
      $window.onbeforeunload = null;
      EventPipe.cancelEvent('plugin.reset');
      EventPipe.cancelEvent('schema.clear');
    });

    this.toggleSidebar = function() {
      this.isExpanded = !this.isExpanded;
    };
  });
