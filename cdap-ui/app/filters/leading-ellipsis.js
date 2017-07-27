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
 * Similar to ellipsis.js filter, but show ellipses in the front instead
 **/
angular.module(PKG.name+'.filters').filter('myLeadingEllipsis', function($filter) {
  return function (input, limit, lastDigitsToShow, additionalFilter, ...additionalFilterArgs) {
    if (typeof input !== 'string') {
      input = input.toString();
    }

    var res = input;

    if (input.length > limit && limit > lastDigitsToShow) {
      var digitsToShow = input.substring(input.length - lastDigitsToShow);
      if (additionalFilter) {
        if (additionalFilterArgs) {
          digitsToShow = $filter(additionalFilter)(digitsToShow, additionalFilterArgs.toString());
        } else {
          digitsToShow = $filter(additionalFilter)(digitsToShow);
        }
      }
      res = '\u2026 ' + digitsToShow;
    }

    return res;
  };
});
