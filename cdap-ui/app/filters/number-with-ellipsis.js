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
 * Similar to the angular 'number' filter and takes the same arguments, but also works
 * on strings with ellipses. e.g. '...11111' -> '...11,111'. If the string doesn't
 * contain ellipses, then just applies the angular 'number' filter.
 **/
angular.module(PKG.name+'.filters').filter('myNumberWithEllipsis', function($filter) {
  return function (input, ...numberFilterArgs) {
    if (input.indexOf('\u2026') !== -1) {
      let ellipsisIndex = input.indexOf('\u2026');
      let numberBeforeEllipsis = input.substring(0, ellipsisIndex);
      if (numberBeforeEllipsis.length > 0) {
        numberBeforeEllipsis = $filter('number')(numberBeforeEllipsis, numberFilterArgs.toString());
      }
      let numberAfterEllipsis = input.substring(ellipsisIndex+1);
      if (numberAfterEllipsis.length > 0) {
        numberAfterEllipsis = $filter('number')(numberAfterEllipsis, numberFilterArgs.toString());
      }
      return numberBeforeEllipsis + '\u2026' + numberAfterEllipsis;

    } else {
      return $filter('number')(input, numberFilterArgs.toString());
    }
  };
});
