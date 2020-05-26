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
  isNullable,
  isComplexType,
  getNonNullableType,
  getComplexTypeName,
} from 'components/AbstractWidget/SchemaEditor/SchemaHelpers';
import uuidV4 from 'uuid/v4';

const flattenUnionType = (complexType, parent) => {
  if (!Array.isArray(complexType)) {
    return [];
  }
  const result = [];
  for (const subType of getNonNullableType(complexType)) {
    if (isComplexType(subType)) {
      const complexTypeName = getComplexTypeName(subType);
      const name = `${parent[parent.length - 1]}-union-${complexTypeName}`;
      result.push({
        id: `id-${uuidV4()}`,
        name,
        type: complexTypeName,
        parent,
        nullable: isNullable(subType),
      });
      result.push(...flattenSubTree(subType, parent.concat([name])));
    } else {
      result.push({
        id: `id-${uuidV4()}`,
        name: `${parent[parent.length - 1]}-union`,
        type: subType,
        parent,
        nullable: isNullable(subType),
      });
    }
  }
  return result;
};

const flattenMapType = (complexType, parent) => {
  if (typeof complexType !== 'object') {
    return [];
  }
  const result = [];
  const { keys, values } = getNonNullableType(complexType);
  const mapKeysId = `${parent[parent.length - 1]}-keys`;
  const mapValuesId = `${parent[parent.length - 1]}-values`;
  if (!isComplexType(keys)) {
    result.push({
      id: `id-${uuidV4()}`,
      name: mapKeysId,
      type: keys,
      parent,
      nullable: isNullable(keys),
    });
  } else {
    const complexTypeName = getComplexTypeName(keys);
    result.push({
      id: `id-${uuidV4()}`,
      name: mapKeysId,
      type: complexTypeName,
      parent,
      nullable: isNullable(keys),
    });
    result.push(...flattenSubTree(keys, parent.concat([mapKeysId])));
  }
  if (!isComplexType(values)) {
    result.push({
      id: `id-${uuidV4()}`,
      name: mapValuesId,
      type: values,
      parent,
      nullable: isNullable(values),
    });
  } else {
    const complexTypeName = getComplexTypeName(values);
    result.push({
      id: `id-${uuidV4()}`,
      name: mapValuesId,
      type: complexTypeName,
      parent,
      nullable: isNullable(values),
    });
    result.push(...flattenSubTree(values, parent.concat([mapValuesId])));
  }
  return result;
};

const flattenEnumType = (complexType, parent) => {
  if (typeof complexType !== 'object') {
    return [];
  }
  const result = [];
  const { symbols } = getNonNullableType(complexType);
  for (const [i, symbol] of symbols.entries()) {
    result.push({
      id: `id-${uuidV4()}`,
      name: `${parent[parent.length - 1]}-${i}-enum-symbol`,
      type: 'enum-symbol',
      parent,
      symbol,
    });
  }
  return result;
};

const flattenArrayType = (complexType, parent) => {
  if (typeof complexType !== 'object') {
    return [];
  }
  const result = [];
  const { items } = getNonNullableType(complexType);
  const nullable = isNullable(items);
  const itemsId = `${parent[parent.length - 1]}-array-items`;
  if (!isComplexType(items)) {
    result.push({
      id: `id-${uuidV4()}`,
      name: itemsId,
      type: getNonNullableType(items),
      parent,
      nullable,
    });
  } else {
    const complexTypeName = getComplexTypeName(items);
    result.push({
      id: `id-${uuidV4()}`,
      type: complexTypeName,
      name: itemsId,
      nullable,
      parent,
    });
    result.push(...flattenSubTree(items, parent.concat([itemsId])));
  }
  return result;
};

const flattenSubTree = (complexType, parent) => {
  const type = getComplexTypeName(complexType);
  switch (type) {
    case 'union':
      return flattenUnionType(complexType, parent);
    case 'map':
      return flattenMapType(complexType, parent);
    case 'enum':
      return flattenEnumType(complexType, parent);
    case 'array':
      return flattenArrayType(complexType, parent);
    case 'record':
      return flattenSchema({ schema: getNonNullableType(complexType) }, parent);
    default:
      return complexType;
  }
};

const flattenFields = (fields, parent) => {
  /**
   * check if it is nullable
   *  check if it is a simple type
   *    if so just add it to the array
   *    else parse the complex type
   * else it is a union type parse and add it to array
   */
  if (!Array.isArray(fields)) {
    return [];
  }
  const result = [];
  for (const field of fields) {
    const nullable = isNullable(field.type);
    const fieldObj = {
      id: `id-${uuidV4()}`,
      name: field.name,
      nullable,
      parent,
    };
    if (!isComplexType(field.type)) {
      fieldObj.type = getNonNullableType(field.type);
      result.push(fieldObj);
    } else {
      fieldObj.type = getComplexTypeName(field.type);
      result.push(fieldObj);
      // flatten the complex type subtree.
      result.push(...flattenSubTree(field.type, parent.concat([field.name])));
    }
  }
  return result;
};

const flattenSchema = (s, parent = ['root']) => {
  const schema = s.schema;
  const op = [];
  if (schema.fields) {
    op.push(...flattenFields(schema.fields, parent));
  }
  return op;
};

export { flattenSchema };
