/*
 * Copyright © 2020 Cask Data, Inc.
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

import {
  IFieldType,
  ISimpleTypeNullable,
  IComplexTypeFieldNullable,
  IComplexType,
} from 'components/AbstractWidget/SchemaEditor/SchemaTypes';
import cloneDeep from 'lodash/cloneDeep';

const isNullable = (type) => {
  if (Array.isArray(type)) {
    return type.find((t) => t === 'null') === 'null';
  }
  return false;
};
const isUnion = (type) => {
  return Array.isArray(type) && !isNullable(type);
};

const getSimpleType = (type) => {
  if (Array.isArray(type)) {
    const nonNullTypes = type.filter((t) => t !== 'null');
    if (nonNullTypes.length === 1) {
      return nonNullTypes[0];
    }
  }
  return type;
};

const isComplexType = (complexType) => {
  if (typeof complexType === 'string') {
    return false;
  }
  switch (complexType.type) {
    case 'record':
    case 'enum':
    case 'array':
    case 'map':
      return true;
    default:
      return isUnion(complexType) ? true : false;
  }
};

const getComplexType = (complexType) => {
  const c = cloneDeep(complexType);
  let type = c.type;
  if (isNullable(type)) {
    type = type.filter((t) => t !== 'null').pop();
  }
  switch (type) {
    case 'record':
    case 'enum':
    case 'array':
    case 'map':
      return type;
    default:
      return isUnion(c) ? 'union' : undefined;
  }
};

export { isNullable, isUnion, isComplexType, getSimpleType, getComplexType };
