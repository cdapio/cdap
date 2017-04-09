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
 * Filter that searches through multiple keys of an object for the search text.
 * This does a case insensitive "contains" match for the search term.
 * @param {Array} input Items to search through.
 * @param {Array} keys Keys to match.
 * @param {String} search Search terms.
 * @return {Array} filtered items.
 */
angular.module(PKG.name+'.filters').filter('myMultiKeySearch', function() {
  return function multiKeySearch(input, keys, search) {
    if (!angular.isArray(keys) || !keys.length || !search) {
      return input;
    }
    search = search.toLowerCase();
    return input.filter(value =>
      keys.filter(key => value[key] &&
       value[key].toLowerCase().indexOf(search) !== -1).length
    );
  };
});
