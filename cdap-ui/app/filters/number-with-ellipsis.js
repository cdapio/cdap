/*
 * Copyright © 2017 Cask Data, Inc.
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
 * Adds comma separator to numeric strings with ellipsis. e.g. '...11111' -> '...11,111'.
 * If the string doesn't contain ellipses, then just returns the number.
 **/
angular.module(PKG.name+'.filters').filter('myNumberWithEllipsis', function() {
  return function (input) {
    if (input.indexOf('\u2026') !== -1) {
      let ellipsisIndex = input.indexOf('\u2026');
      let numberBeforeEllipsis = input.substring(0, ellipsisIndex);
      if (numberBeforeEllipsis.length > 0) {
        numberBeforeEllipsis = parseInt(numberBeforeEllipsis, 10).toLocaleString('en');
      }
      let numberAfterEllipsis = input.substring(ellipsisIndex+1);
      if (numberAfterEllipsis.length > 0) {
        numberAfterEllipsis = parseInt(numberAfterEllipsis, 10).toLocaleString('en');
      }
      return numberBeforeEllipsis + '\u2026' + numberAfterEllipsis;

    } else {
      return parseInt(input, 10).toLocaleString('en');
    }
  };
});
