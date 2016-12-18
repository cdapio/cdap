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

export function renameColumn(table, oldName, newName) {
  let formattedData = table.map((row) => {
    let newObj = Object.assign({}, row);
    newObj[newName] = newObj[oldName];
    delete newObj[oldName];
    return newObj;
  });

  return formattedData;
}

export function dropColumn(table, columnToDrop) {
  let formattedData = table.map((row) => {
    let newObj = Object.assign({}, row);
    delete newObj[columnToDrop];
    return newObj;
  });

  return formattedData;
}

export function splitColumn(table, delimiter, columnToSplit, firstSplit, secondSplit) {
  let formattedData = table.map((row) => {
    let newObj = Object.assign({}, row);

    let split = newObj[columnToSplit] || '';
    let index = split.indexOf(delimiter);

    newObj[firstSplit] = split.slice(0,index);
    newObj[secondSplit] = split.slice(index+1);

    return newObj;
  });

  return formattedData;
}

export function mergeColumn(table, joinKey, firstColumn, secondColumn, columnName) {
  let formattedData = table.map((row) => {
    let newObj = Object.assign({}, row);
    let merged;
    if (!newObj[firstColumn] && !newObj[secondColumn]) {
      merged = '';
    } else {
      merged = newObj[firstColumn].concat(joinKey, newObj[secondColumn]);
    }
    newObj[columnName] = merged;

    return newObj;
  });

  return formattedData;
}

export function uppercaseColumn(table, column) {
  let formattedData = table.map((row) => {
    let newObj = Object.assign({}, row);

    if (!newObj[column]) {
      return '';
    }

    newObj[column] = newObj[column].toUpperCase();

    return newObj;
  });

  return formattedData;
}

export function lowercaseColumn(table, column) {
  let formattedData = table.map((row) => {
    let newObj = Object.assign({}, row);

    if (!newObj[column]) {
      return '';
    }

    newObj[column] = newObj[column].toLowerCase();

    return newObj;
  });

  return formattedData;
}

export function titlecaseColumn(table, column) {
  let formattedData = table.map((row) => {
    let newObj = Object.assign({}, row);

    if (!newObj[column]) {
      return '';
    }

    let titleCase = newObj[column].split(' ')
      .map((word) => {
        if (!word) { return word; }
        return word[0].toUpperCase() + word.slice(1).toLowerCase();
      })
      .join(' ');

    newObj[column] = titleCase;

    return newObj;
  });

  return formattedData;
}

export function substringColumn(table, columnToSub, begin, end, columnName) {
  let formattedData = table.map((row) => {
    let newObj = Object.assign({}, row);

    if (!newObj[columnToSub]) {
      return '';
    }

    newObj[columnName] = newObj[columnToSub].substr(begin, end);

    return newObj;
  });

  return formattedData;
}
