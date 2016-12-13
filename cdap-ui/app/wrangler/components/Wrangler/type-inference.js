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

import sortBy from 'lodash/sortBy';

/*
 * This function will attempt to infer type from a string
 * one of integer, float, boolean, string
 **/
export function inferType(val) {
  if (typeof val !== 'string' && typeof val !==  'undefined') {
    throw 'Input is not a string: ' + typeof val;
  }

  // check if boolean
  if (val === 'true' || val === 'false') {
    return 'boolean';
  }

  // check if integer or float
  let parsedNumber = parseFloat(val);
  if (parsedNumber.toString() === val && !isNaN(parsedNumber)) {
    if (parsedNumber % 1 === 0) {
      return 'int';
    } else {
      return 'float';
    }
  }

  // catch all
  return 'string';
}

export function inferColumn(table, columnName) {
  let count = {};
  const THRESHOLD = 0.75;

  table.forEach((row) => {
    let type = inferType(row[columnName]);

    if (!count[type]) {
      count[type] = 1;
    } else {
      count[type]++;
    }
  });

  let types = Object.keys(count).map((type) => {
    return {
      type,
      count: count[type]
    };
  });

  types = sortBy(types, ['count']);

  let maxType = types[types.length - 1];

  // Check if maxType exceed THRESHOLD
  let ratio = maxType.count / table.length;

  return ratio > THRESHOLD ? maxType.type : 'string';
}
