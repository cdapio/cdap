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

angular.module(PKG.name+'.filters').filter('myNumber', function() {
  return function (input, precision) {
    if (input<1 || isNaN(parseFloat(input)) || !isFinite(input)) {
      return '0';
    }

    if (typeof precision === 'undefined') {
      precision = input > 1023 ? 1 : 0;
    }

    var number = Math.floor(Math.log(input) / Math.log(1000));
    return (input / Math.pow(1000, Math.floor(number))).toFixed(precision) +
      '' + ['', 'k', 'M', 'G', 'T', 'P'][number];

  };
});
