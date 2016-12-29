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

var d3 = require('d3');

export function createBucket(array, columnName, columnType) {
  let data;

  switch (columnType) {
    case 'string':
      data = array.map((row) => {
        return row[columnName] ? row[columnName].length : 0;
      });
      break;
    case 'float':
    case 'int':
      data = array.map((row) => {
        return row[columnName] ? parseInt(row[columnName], 10) : 'null';
      });
      break;
    case 'boolean':
      // ??????
      break;
  }

  // let extent = d3.extent(data);

  // TODO: FIX THIS after downgrade to d3 v3

  // let x = d3.scale.linear()
  //   .domain(extent);

  let histogram = d3.layout.histogram()
    .bins(5);

  let histogramData = histogram(data);
  let resultData = histogramData.map((bucket) => bucket.length);
  let labels = histogramData.map((bucket) => {
    return `${bucket.x0} - ${bucket.x1}`;
  });

  return {
    data: resultData,
    labels
  };
}

// function _createBooleanDistribution(array, columnName) {
//   let distribution = {
//     'true': 0,
//     'false': 0,
//     'null': 0,
//     'error': 0
//   };

//   array.forEach((row) => {
//     switch (row[columnType]) {
//       case 'true':
//         distribution['true']++;
//         break;
//       case 'false':
//         distribution['false']++;
//         break;
//       case '':
//         distribution['null']++;
//         break;
//       default:
//         distribution['error']++;
//     }
//   });



// }

