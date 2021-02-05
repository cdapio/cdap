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
  INode,
  parseUnionType,
  parseArrayType,
  parseEnumType,
  parseMapType,
  IOrderedChildren,
  parseComplexType,
} from 'components/AbstractWidget/SchemaEditor/Context/SchemaParser';
import {
  logicalTypes,
  defaultTimeStampTypeProperties,
  defaultDecimalTypeProperties,
  defaultTimeTypeProperties,
  defaultDateTypeProperties,
  defaultDateTimeTypeProperties,
  defaultArrayType,
  defaultEnumType,
  defaultMapType,
  defaultRecordType,
  defaultUnionType,
  InternalTypesEnum,
  AvroSchemaTypesEnum,
} from 'components/AbstractWidget/SchemaEditor/SchemaConstants';
import isEmpty from 'lodash/isEmpty';

function getInternalType(tree: INode) {
  const hasChildren = tree.children ? Object.keys(tree.children).length > 0 : false;
  if (tree.internalType === InternalTypesEnum.RECORD_SIMPLE_TYPE && hasChildren) {
    return InternalTypesEnum.RECORD_COMPLEX_TYPE_ROOT;
  }
  if (tree.internalType === InternalTypesEnum.RECORD_COMPLEX_TYPE_ROOT && !hasChildren) {
    return InternalTypesEnum.RECORD_SIMPLE_TYPE;
  }
  if (tree.internalType === InternalTypesEnum.UNION_SIMPLE_TYPE && hasChildren) {
    return InternalTypesEnum.UNION_COMPLEX_TYPE_ROOT;
  }
  if (tree.internalType === InternalTypesEnum.UNION_COMPLEX_TYPE_ROOT && !hasChildren) {
    return InternalTypesEnum.UNION_SIMPLE_TYPE;
  }
  if (tree.internalType === InternalTypesEnum.ARRAY_SIMPLE_TYPE && hasChildren) {
    return InternalTypesEnum.ARRAY_COMPLEX_TYPE_ROOT;
  }
  if (tree.internalType === InternalTypesEnum.ARRAY_COMPLEX_TYPE_ROOT && !hasChildren) {
    return InternalTypesEnum.ARRAY_SIMPLE_TYPE;
  }
  if (tree.internalType === InternalTypesEnum.MAP_KEYS_SIMPLE_TYPE && hasChildren) {
    return InternalTypesEnum.MAP_KEYS_COMPLEX_TYPE_ROOT;
  }
  if (tree.internalType === InternalTypesEnum.MAP_KEYS_COMPLEX_TYPE_ROOT && hasChildren) {
    return InternalTypesEnum.MAP_KEYS_SIMPLE_TYPE;
  }
  if (tree.internalType === InternalTypesEnum.MAP_VALUES_SIMPLE_TYPE && hasChildren) {
    return InternalTypesEnum.MAP_VALUES_COMPLEX_TYPE_ROOT;
  }
  if (tree.internalType === InternalTypesEnum.MAP_VALUES_COMPLEX_TYPE_ROOT && hasChildren) {
    return InternalTypesEnum.MAP_VALUES_SIMPLE_TYPE;
  }
  return tree.internalType;
}

const branchCount = (tree: INode): number => {
  let count = 0;
  if (tree && !isEmpty(tree.children) && Object.keys(tree.children).length) {
    // skip 'order' array which is under children.
    const children = Object.values(tree.children).filter((child) => !Array.isArray(child));
    count += children.length;
    children.forEach((child: INode) => {
      count += branchCount(child);
    });
  }
  return count;
};

const initChildren = (type): IOrderedChildren => {
  switch (type) {
    case AvroSchemaTypesEnum.ARRAY:
      return parseArrayType(defaultArrayType);
    case AvroSchemaTypesEnum.ENUM:
      return parseEnumType(defaultEnumType);
    case AvroSchemaTypesEnum.MAP:
      return parseMapType(defaultMapType);
    case AvroSchemaTypesEnum.RECORD:
      return parseComplexType(defaultRecordType);
    case AvroSchemaTypesEnum.UNION:
      return parseUnionType(defaultUnionType);
    default:
      return;
  }
};

const initTypeProperties = (tree: INode) => {
  if (logicalTypes.indexOf(tree.type) === -1) {
    return {};
  }
  switch (tree.type) {
    case AvroSchemaTypesEnum.DECIMAL:
      return defaultDecimalTypeProperties;
    case AvroSchemaTypesEnum.TIME:
      return defaultTimeTypeProperties;
    case AvroSchemaTypesEnum.TIMESTAMP:
      return defaultTimeStampTypeProperties;
    case AvroSchemaTypesEnum.DATE:
      return defaultDateTypeProperties;
    case AvroSchemaTypesEnum.DATETIME:
      return defaultDateTimeTypeProperties;
    default:
      return {};
  }
};

export { getInternalType, branchCount, initChildren, initTypeProperties };
