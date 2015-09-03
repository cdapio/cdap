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

/**
 * myNavbar
 */

angular.module(PKG.name+'.commons').directive('myNavbar',

function myNavbarDirective ($dropdown, myAuth, caskTheme, MY_CONFIG) {
  return {
    restrict: 'A',
    templateUrl: 'navbar/navbar.html',
    controller: 'navbarCtrl',
    link: function (scope, element) {

      var toggles = element[0].querySelectorAll('a.dropdown-toggle');
      // Namespace dropdown
      $dropdown(angular.element(toggles[0]), {
        template: 'navbar/namespace.html',
        animation: 'am-flip-x',
        scope: scope
      });

      // Settings dropdown
      $dropdown(angular.element(toggles[1]), {
        template: 'navbar/dropdown.html',
        animation: 'am-flip-x',
        placement: 'bottom-right',
        scope: scope
      });

      scope.logout = myAuth.logout;
      // If we plan later we could add multiple
      // themes but not in the near future.
      // scope.theme = caskTheme;
      scope.securityEnabled = MY_CONFIG.securityEnabled;
    }
  };
});
