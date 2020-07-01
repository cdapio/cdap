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
  IComplexTypeNames,
  ISimpleType,
  ILogicalTypeNames,
} from 'components/AbstractWidget/SchemaEditor/SchemaTypes';
import { logicalTypeToSimpleTypeMap } from 'components/AbstractWidget/SchemaEditor/SchemaConstants';
import cloneDeep from 'lodash/cloneDeep';

const displayTypes: Array<ISimpleType | IComplexTypeNames | ILogicalTypeNames> = [
  'array',
  'enum',
  'map',
  'record',
  'union',
  'boolean',
  'bytes',
  'date',
  'decimal',
  'double',
  'float',
  'int',
  'long',
  'number',
  'string',
  'time',
  'timestamp-micros',
  'date',
  'time-micros',
  'decimal',
];

/**
 * Checks if the current avro type is nullable.
 * @param type any avro type (simple/complex types).
 */
const isNullable = (type) => {
  if (Array.isArray(type)) {
    return type.find((t) => t === 'null') === 'null';
  }
  return false;
};

const isUnion = (type) => {
  return Array.isArray(type) && !isNullable(type);
};
/**
 * If the type is nullable get the non-null type for further processing.
 * @param type any valid avro type
 */
const getNonNullableType = (type) => {
  if (Array.isArray(type) && !isUnion(type)) {
    const nonNullTypes = type.filter((t) => t !== 'null');
    if (nonNullTypes.length === 1 && type.length - 1 === nonNullTypes.length) {
      return nonNullTypes[0];
    }
  }
  return type;
};
/**
 * Helps in getting the simple type or underlying type in a logical type.
 * @param type valid simple/logical avro type
 */
const getSimpleType = (type) => {
  if (typeof type === 'string') {
    return type;
  }
  if (type && type.logicalType) {
    return logicalTypeToSimpleTypeMap[type.logicalType];
  }
  return type;
};
/**
 * Utility to check if the current type is a complex type to tranverse further
 * into the schema tree.
 * @param complexType any valid complex avro type. (map, array, record, union and enum)
 */
const isComplexType = (complexType) => {
  const nullable = isNullable(complexType);
  let type = complexType;
  if (nullable) {
    type = complexType.filter((t) => t !== 'null').pop();
  }
  if (typeof type === 'string') {
    return false;
  }
  switch (type.type) {
    case 'record':
    case 'enum':
    case 'array':
    case 'map':
      return true;
    default:
      return isUnion(complexType) ? true : false;
  }
};

const isDisplayTypeLogical = ({ type }) => {
  switch (type) {
    case 'decimal':
    case 'date':
    case 'time':
    case 'timestamp':
      return true;
    default:
      return false;
  }
};

const isDisplayTypeComplex = ({ type }) => {
  switch (type) {
    case 'record':
    case 'enum':
    case 'union':
    case 'map':
    case 'array':
      return true;
    default:
      return isDisplayTypeLogical({ type }) || false;
  }
};

/**
 * Utility function to get the complex type names.
 * @param complexType any valid complex avro type.
 */
const getComplexTypeName = (complexType): IComplexTypeNames => {
  const c = complexType;
  let type;
  if (isNullable(complexType)) {
    type = complexType.filter((t) => t !== 'null').pop();
    type = type.type;
  } else {
    type = cloneDeep(c.type);
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

const isFlatRowTypeComplex = (typeName: ISimpleType | IComplexTypeNames) => {
  switch (typeName) {
    case 'string':
    case 'boolean':
    case 'bytes':
    case 'double':
    case 'float':
    case 'int':
    case 'long':
    case 'number':
    case 'string':
      return false;
    default:
      return true;
  }
};

export {
  isNullable,
  isUnion,
  isComplexType,
  getNonNullableType,
  getComplexTypeName,
  displayTypes,
  getSimpleType,
  isFlatRowTypeComplex,
  isDisplayTypeLogical,
  isDisplayTypeComplex,
};
