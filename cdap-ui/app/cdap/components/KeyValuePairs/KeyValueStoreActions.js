/*
* Copyright © 2016-2018 Cask Data, Inc.
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

import {getDefaultKeyValuePair} from 'components/KeyValuePairs/KeyValueStore';
import {convertMapToKeyValuePairs, convertKeyValuePairsToMap} from 'services/helpers';

const KeyValueStoreActions = {
  setKey: 'SET-KEY',
  setVal: 'SET-VALUE',
  setProvided: 'SET-PROVIDED',
  addPair: 'ADD-PAIR',
  deletePair: 'DELETE-PAIR',
  onReset: 'ON-RESET',
  onUpdate: 'ON-UPDATE'
};

const convertMapToKeyValuePairsObj = (obj) => {
  let keyValuePairsObj = {
    pairs: convertMapToKeyValuePairs(obj)
  };
  if (!keyValuePairsObj.pairs.length) {
    keyValuePairsObj.pairs.push(getDefaultKeyValuePair());
  }
  return keyValuePairsObj;
};

const convertKeyValuePairsObjToMap = (keyValues) => {
  return convertKeyValuePairsToMap(keyValues.pairs || []);
};

const keyValuePairsHaveMissingValues = (keyValues) => {
  if (keyValues.pairs) {
    return keyValues.pairs.some((keyValuePair) => {
      if (keyValuePair.notDeletable && keyValuePair.provided) { return false; }
      let emptyKeyField = (keyValuePair.key.length === 0);
      let emptyValueField = (keyValuePair.value.length === 0);
      // buttons are disabled when either the key or the value of a pair is empty, but not both
      return (emptyKeyField && !emptyValueField) || (!emptyKeyField && emptyValueField);
    });
  }
  return false;
};

export default KeyValueStoreActions;
export {
  convertMapToKeyValuePairsObj,
  convertKeyValuePairsObjToMap,
  keyValuePairsHaveMissingValues
};
