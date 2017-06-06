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

angular.module(PKG.name+'.filters').filter('myRemoveCamelcase', function() {
  return function camelToTitle(input) {
    // Handle "HBase" or "OCaml" case with no change to input.
    var result = input.match(/^[A-Z][A-Z][a-z]+/);
    // FIXME: specifically for 'JavaScript' case which we don't want to split.
    var jsresult = input.match(/^JavaScript?/);

    if (result || jsresult) {
      return input;
    }

    // FIXME: Hardcoded for now since we don't want to modify algorithm for these 2 cases.
    // Also we don't know where exactly to split either
    // Ideally this should come from the backend
    if (input === 'CDCHBase') {
      return 'CDC HBase';
    }

    if (input === 'ChangeTrackingSQLServer') {
      return 'Change Tracking SQLServer';
    }

    return input.replace(/([a-z])([A-Z])/g, '$1 $2')
    .replace(/([A-Z])([a-z])/g, ' $1$2')
    .replace(/\ +/g, ' ').trim();
	};
});
