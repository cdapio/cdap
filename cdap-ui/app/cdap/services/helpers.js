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

import isObject from 'lodash/isObject';
import numeral from 'numeral';


/*
  Purpose: Query a json object or an array of json objects
  Return: Returns undefined if property is not defined(never set) and
          and a valid value (including null) if defined.
  Usage:
    var obj1 = [
      {
        p1: 'something',
        p2: {
          p21: 'angular',
          p22: 21,
          p23: {
            p231: 'ember',
            p232: null
          }
        },
        p3: 1296,
        p4: [1, 2, 3],
        p5: null
      },
      {
        p101: 'somethingelse'
      }
    ]
    1. query(obj1, 0, 'p1') => 'something'
    2. query(obj1, 0, 'p2', 'p22') => 21
    3. query(obj1, 0, 'p2', 'p32') => { p231: 'ember'}
    4. query(obj1, 0, 'notaproperty') => undefined
    5. query(obj1, 0, 'p2', 'p32', 'somethingelse') => undefined
    6. query(obj1, 1, 'p2', 'p32') => undefined
    7. query(obj1, 0, 'p2', 'p23', 'p232') => null
    8. query(obj1, 0, 'p5') => null
 */

function objectQuery(obj) {
  if (!isObject(obj)) {
    return null;
  }
  for (var i = 1; i < arguments.length; i++) {
    if (!isObject(obj)) {
      return undefined;
    }
    obj = obj[arguments[i]];
  }
  return obj;
}
export const HUMANREADABLESTORAGE = 'STORAGE';
function humanReadableNumber(num, type) {
  if (typeof num !== 'number') {
    return num;
  }

  switch(type) {
    case HUMANREADABLESTORAGE:
      return convertBytesToHumanReadable(num);
    default:
      return numeral(num).format('0,0');
  }

}

function convertBytesToHumanReadable(bytes) {
  if (!bytes || typeof bytes !== 'number') {
    return bytes;
  }
  return numeral(bytes).format('0.000b');
}

function isDescendant(parent, child) {
  var node = child.parentNode;
  while (node != null) {
    if (node == parent) {
      return true;
    }
    node = node.parentNode;
  }
  return false;
}

function getArtifactNameAndVersion (nameWithVersion) {
  // core-plugins-3.4.0-SNAPSHOT.jar
  // extracts version from the jar file name. We then get the name of the artifact (that is from the beginning till version beginning)
  let regExpRule = new RegExp('(\\d+)(?:\\.(\\d+))?(?:\\.(\\d+))?(?:[.\\-](.*))?$');
  let version = regExpRule.exec(nameWithVersion)[0];
  let name = nameWithVersion.substr(0, nameWithVersion.indexOf(version) -1);
  return { version, name };
}


function insertAt(arr, index, element) {
  return [
    ...arr.slice(0, index + 1),
    element,
    ...arr.slice(index + 1, arr.length)
  ];
}

function removeAt(arr, index) {
  return [
    ...arr.slice(0, index),
    ...arr.slice(index + 1, arr.length)
  ];
}

export {
  objectQuery,
  convertBytesToHumanReadable,
  humanReadableNumber,
  isDescendant,
  getArtifactNameAndVersion,
  insertAt,
  removeAt
};
