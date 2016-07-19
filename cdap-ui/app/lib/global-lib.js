/*
 * Copyright © 2016 Cask Data, Inc.
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

angular.module(`${PKG.name}.commons`)
  .factory('d3', function ($window) {
    return $window.d3;
  })
  .factory('c3', function ($window) {
    return $window.c3;
  })
  .factory('Redux', function($window) {
    return $window.Redux;
  })
  .factory('ReduxThunk', function($window) {
    return $window.ReduxThunk;
  })
  .factory('js_beautify', function ($window) {
    return $window.js_beautify;
  })
  .factory('esprima', function ($window) {
    return $window.esprima;
  })
  .factory('avsc', function ($window) {
    return $window.avsc;
  })
  .factory('moment', function($window) {
    return $window.moment;
  });
