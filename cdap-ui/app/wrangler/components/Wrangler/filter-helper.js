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

export function filterData(data, filterFunction, column, filterBy, ignoreCase) {
  function _equal(row) {
    let columnData = row[column];
    let filterData = filterBy;

    if (ignoreCase) {
      columnData = columnData.toLowerCase();
      filterData = filterData.toLowerCase();
    }

    return columnData === filterData;
  }

  function _notEqual(row) {
    let columnData = row[column];
    let filterData = filterBy;

    if (ignoreCase) {
      columnData = columnData.toLowerCase();
      filterData = filterData.toLowerCase();
    }

    return columnData !== filterData;
  }

  function _lessThan(row) {
    return parseFloat(row[column]) < parseFloat(filterBy);
  }

  function _greaterThan(row) {
    return parseFloat(row[column]) > parseFloat(filterBy);
  }

  function _lessEqualThan(row) {
    return parseFloat(row[column]) <= parseFloat(filterBy);
  }

  function _greaterEqualThan(row) {
    return parseFloat(row[column]) >= parseFloat(filterBy);
  }

  function _startsWith(row) {
    let columnData = row[column];
    let filterData = filterBy;

    if (ignoreCase) {
      columnData = columnData.toLowerCase();
      filterData = filterData.toLowerCase();
    }

    return columnData.substr(0, filterData.length) === filterData;
  }

  function _endsWith(row) {
    let columnData = row[column];
    let filterData = filterBy;

    if (ignoreCase) {
      columnData = columnData.toLowerCase();
      filterData = filterData.toLowerCase();
    }

    let position = columnData.length - filterData.length;
    return columnData.substr(position) === filterData;
  }

  function _contains(row) {
    const ignoreCase = this.state.filterIgnoreCase;
    let columnData = row[column];
    let filterData = filterBy;

    if (ignoreCase) {
      columnData = columnData.toLowerCase();
      filterData = filterData.toLowerCase();
    }

    return columnData.indexOf(filterData) !== -1;
  }

  const functionsMap = {
    '=': _equal,
    '!=': _notEqual,
    '<': _lessThan,
    '>': _greaterThan,
    '<=': _lessEqualThan,
    '>=': _greaterEqualThan,
    'startsWith': _startsWith,
    'endsWith': _endsWith,
    'contains': _contains
  };

  return data.filter(functionsMap[filterFunction].bind(this));
}
