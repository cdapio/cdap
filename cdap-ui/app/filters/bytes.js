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

// adapted from https://gist.github.com/thomseddon/3511330

angular.module(PKG.name+'.filters').filter('bytes', function() {
  return function(bytes, precision) {
    if (bytes<1 || isNaN(parseFloat(bytes)) || !isFinite(bytes)) {
      return '0b';
    }
    if (typeof precision === 'undefined') {
      precision = bytes>1023 ? 1 : 0;
    }
    var number = Math.floor(Math.log(bytes) / Math.log(1024));
    return (bytes / Math.pow(1024, Math.floor(number))).toFixed(precision) +
      '' + ['b', 'kB', 'MB', 'GB', 'TB', 'PB'][number];
  };
});
