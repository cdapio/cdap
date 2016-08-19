/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

angular.module(PKG.name + '.services')
  .factory('myAlertOnValium', function($alert, $window) {
    var isAnAlertOpened = false,
        alertObj;

    var SUCCESS_ALERT_DURATION = 3; // duration amount in seconds

    function show(obj) {
      if (alertObj) {
        alertObj.hide();
      }

      obj.duration = obj.type === 'success' ? SUCCESS_ALERT_DURATION : false;
      alertObj = $alert(obj);
      if (obj.templateUrl) {
        alertObj.$scope.templateScope = obj.templateScope;
      }

       // Scroll to top so that user doesn't miss an alert
      $window.scrollTo(0, 0);
    }
    function destroy() {
      alertObj.hide();
    }
    function getisAnAlertOpened() {
      return isAnAlertOpened;
    }

    return {
      show: show,
      isAnAlertOpened: getisAnAlertOpened,
      destroy: destroy
    };
  });
