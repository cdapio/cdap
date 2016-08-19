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

angular.module(PKG.name + '.services')
  .factory('ProgramsHelpers', function() {
    // These two functions are utterly trivial and should not exist.
    // We will use these function as a way to understand what entity name should be
    // used in what places. For instance in logs and metrics singluar form is required
    // in runs and status plural form is required. Eventually when we fix UI to use
    // one single entity name then it will be easier for us to clean this across UI.
    const getSingularName = (entity) => {
      return angular.isString(entity) &&
        entity.slice(-1) === 's' ? entity.slice(0, entity.length - 1) : entity;
    };
    const getPluralName = (entity) => {
      return entity + 's';
    };
    return {
      getSingularName: getSingularName,
      getPluralName: getPluralName
    };
  });
