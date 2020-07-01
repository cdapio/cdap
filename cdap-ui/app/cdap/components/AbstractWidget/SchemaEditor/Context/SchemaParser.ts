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
import { IInternalFieldType } from 'components/AbstractWidget/SchemaEditor/EditorTypes';
import {
  ISchemaType,
  IDisplayType,
  IFieldType,
  IFieldTypeNullable,
  ILogicalTypeBase,
} from 'components/AbstractWidget/SchemaEditor/SchemaTypes';
import {
  getComplexTypeName,
  isNullable,
  isComplexType,
  getNonNullableType,
  getSimpleType,
} from 'components/AbstractWidget/SchemaEditor/SchemaHelpers';
import uuidV4 from 'uuid/v4';
import { InternalTypesEnum } from '../SchemaConstants';

type ITypeProperties = Record<string, any>;

/**
 * A generic node of the tree.
 * name, id, nullable and type - directly translate to the avro schema
 * children - is a map of child-id and the child node. In cases like records and enums
 * where the order of children needs to be maintained, the map will have a static
 * 'order' array.
 * internalType - purely used for presentation.
 * typeProperties - Defines the properties of the type. Right now this takes in
 * symbols in the enum and precision, scale in decimal types.
 */
interface INode {
  name?: string;
  children?: IOrderedChildren;
  id: string;
  internalType: IInternalFieldType;
  nullable?: boolean;
  type?: IDisplayType;
  typeProperties?: ITypeProperties;
}

type IOrderedChildren = Record<string, INode> | Record<'order', string[]>;

/**
 * {
 *  [child-id1]: {
 *    id: child-id1,
 *    internalType: 'union-simple-type',
 *    type: 'string'
 *  }
 * }
 * @param type avro union type
 * ['string']
 */
function parseUnionType(type): IOrderedChildren {
  const result: IOrderedChildren = {
    order: [],
  };
  for (const subType of type) {
    const id = uuidV4();
    result.order.push(id);
    if (isComplexType(subType)) {
      const typeName = getComplexTypeName(subType);
      result[id] = {
        id,
        type: typeName,
        internalType: InternalTypesEnum.UNION_COMPLEX_TYPE_ROOT,
        children: parseComplexType(subType),
      };
    } else {
      result[id] = {
        id,
        type: subType,
        nullable: false,
        internalType: InternalTypesEnum.UNION_SIMPLE_TYPE,
      };
    }
  }
  return result;
}
/**
 * @returns
 * {
 *  [child-id1]: {
 *    internalType: 'array-simple-type',
 *    type: 'string'
 *  }
 * }
 * @param type avro array type
 * {
 *  type: 'array',
 *  items: 'string'
 * }
 */
function parseArrayType(type): IOrderedChildren {
  const nullable = isNullable(type);
  const t = getNonNullableType(type);
  const id = uuidV4();
  if (t.items && !isComplexType(t.items)) {
    return {
      [id]: {
        internalType: InternalTypesEnum.ARRAY_SIMPLE_TYPE,
        id,
        nullable: isNullable(t.items),
        type: getNonNullableType(t.items),
      },
    };
  }
  return {
    [id]: {
      internalType: InternalTypesEnum.ARRAY_COMPLEX_TYPE_ROOT,
      id,
      nullable,
      type: getComplexTypeName(t.items),
      children: parseComplexType(t.items),
    },
  };
}

/**
 * @returns
 * {
 *  order: [child-id1, child-id2,...]
 *  [child-id1]: {
 *    id: child-id1,
 *    internalType: 'enum-symbol',
 *    typeProperties: {
 *      symbol: 'symbol1'
 *    }
 *  },
 *  [child-id2]: {
 *    id: child-id2,
 *    internalType: 'enum-symbol',
 *    typeProperties: {
 *      symbol: 'symbol2'
 *    }
 *  }
 * }
 * @param type avro enum type
 * {
 *  type: 'enum',
 *  symbols: ['symbol1', 'symbol2', ....]
 * }
 */
function parseEnumType(type): IOrderedChildren {
  const nullable = isNullable(type);
  const t = getNonNullableType(type);
  const result = {
    order: [],
  };
  for (const symbol of t.symbols) {
    const id = uuidV4();
    result.order.push(id);
    result[id] = {
      id,
      internalType: InternalTypesEnum.ENUM_SYMBOL,
      nullable,
      typeProperties: {
        symbol,
      },
    };
  }
  return result;
}

function getMapSubType(type, internalTypeName): INode {
  const id = uuidV4();
  if (!isComplexType(type)) {
    return {
      id,
      internalType: internalTypeName.simpleType,
      nullable: isNullable(type),
      type: getNonNullableType(type),
    };
  } else {
    const complexType = getComplexTypeName(type);
    const nullable = isNullable(type);
    return {
      children: parseComplexType(type),
      id,
      internalType: internalTypeName.complexType,
      type: complexType,
      nullable,
    };
  }
}
/**
 * @returns
 * {
 *   [child-id1]: {
 *    "id": child-id1,
 *    "internalType": "map-keys-simple-type",
 *    "type": "string"
 *  },
 *   [child-id2] {
 *    "id": child-id2,
 *    "internalType": "map-values-simple-type",
 *    "nullable": false,
 *    "type": "string"
 *   }
 * }
 * @param type avro map type
 * {
 *  type: {
 *    keys: 'string',
 *    values: 'string',
 *  }
 * }
 */
function parseMapType(type): IOrderedChildren {
  const t = getNonNullableType(type);
  const keysType = t.keys;
  const valuesType = t.values;
  const result: Record<string, INode> = {};
  const mapKeysSubType = getMapSubType(keysType, {
    simpleType: InternalTypesEnum.MAP_KEYS_SIMPLE_TYPE,
    complexType: InternalTypesEnum.MAP_KEYS_COMPLEX_TYPE_ROOT,
  });
  const mapValuesSubType = getMapSubType(valuesType, {
    simpleType: InternalTypesEnum.MAP_VALUES_SIMPLE_TYPE,
    complexType: InternalTypesEnum.MAP_VALUES_COMPLEX_TYPE_ROOT,
  });
  result[mapKeysSubType.id] = mapKeysSubType;
  result[mapValuesSubType.id] = mapValuesSubType;
  return result;
}
/**
 * @returns -
 * {
 *  [child-id1]:{
 *    id: child-id1,
 *    internalType: 'record-field-simple-type',
 *    type: 'string',
 *  },
 *  order: [child-id1]
 * }
 * @param type - avro record type
 * {
 *  name: 'record-name',
 *  type: 'record',
 *  fields: [ field1, field2 ...]
 * }
 */
function parseRecordType(type): IOrderedChildren {
  const t = getNonNullableType(type);
  const result = {
    order: [],
  };
  for (const field of t.fields) {
    const child = parseSubTree(field);
    result.order.push(child.id);
    result[child.id] = child;
  }
  return result;
}

function parseComplexType(type): IOrderedChildren {
  const complexTypeName = getComplexTypeName(type);
  let record: IOrderedChildren = {};
  switch (complexTypeName) {
    case 'enum':
      record = parseEnumType(type);
      break;
    case 'array':
      record = parseArrayType(type);
      break;
    case 'record':
      record = parseRecordType(type);
      break;
    case 'union':
      record = parseUnionType(type);
      break;
    case 'map':
      record = parseMapType(type);
      break;
    default:
      record = {};
  }
  return record;
}

/**
 * The logical type is similar to a complex type. The only difference is it doesn't have
 *  children and will have type properties that map the logical property to underlying type.
 * @param field - field in the schema.
 */
function checkForLogicalType(field: IFieldType | IFieldTypeNullable) {
  let type = field.type;
  type = getNonNullableType(type) as ILogicalTypeBase;
  switch (type.logicalType) {
    case 'decimal':
      return {
        typeProperties: {
          type: 'bytes',
          logicalType: type.logicalType,
          precision: type.precision,
          scale: type.scale,
        },
      };
    case 'date':
      return {
        typeProperties: {
          type: 'int',
          logicalType: type.logicalType,
        },
      };
    case 'time-micros':
      return {
        typeProperties: {
          type: 'long',
          logicalType: type.logicalType,
        },
      };
    case 'timestamp-micros':
      return {
        typeProperties: {
          type: 'long',
          logicalType: type.logicalType,
        },
      };
    default:
      return {
        typeProperties: {
          doc: field.doc,
          aliases: field.aliases,
        },
      };
  }
}

/**
 * Function to parse fields in a record type. Can be a simple field or a complex record type.
 * @param field - A field in the record type.
 */
function parseSubTree(field: IFieldType | IFieldTypeNullable): INode {
  const { type, name } = field;
  const nullable = isNullable(type);
  const complexType = isComplexType(type);
  const t = getNonNullableType(type);
  if (!complexType) {
    return {
      name,
      id: uuidV4(),
      internalType: InternalTypesEnum.RECORD_SIMPLE_TYPE,
      nullable,
      type: getSimpleType(t),
      ...checkForLogicalType(field),
    };
  }
  return {
    name,
    children: parseComplexType(type),
    id: uuidV4(),
    internalType: InternalTypesEnum.RECORD_COMPLEX_TYPE_ROOT,
    nullable,
    type: getComplexTypeName(t),
    typeProperties: {
      doc: t.doc,
      aliases: t.aliases,
    },
  };
}

/**
 * Parser to convert the avro schema JSON to internal representation of a tree.
 *
 * Each node has a type for display and an internal type to determine how rendering
 * happens. Based on the internal type presentation layer decides to nest/render rows.
 * @param avroSchema avro schema JSON
 * @param name default name of the schema.
 * @return The return is a tree. Will always be a schema type with record type. Can have deep nesting
 * depending upon the schema complexity.
 * {
 *  id: xxx,
 *  internalType: 'schema',
 *  type: 'record',
 *  children: {
 *    order: [array-of-child-ids],
 *    [child-id1]: child-id1 node,
 *    [child-id2]: child-id2 node,
 *  }
 * }
 */
function parseSchema(avroSchema: ISchemaType, name = 'etlSchemaBody'): INode {
  const fields = avroSchema.schema.fields;
  const root: INode = {
    name,
    internalType: 'schema', // The 'schema' is only used for top level schema.
    type: 'record',
    id: uuidV4(),
    children: {
      order: [],
    } as IOrderedChildren,
  };
  for (const field of fields) {
    const child = parseSubTree(field);
    if (Array.isArray(root.children.order)) {
      root.children.order.push(child.id);
    }
    root.children[child.id] = child;
  }
  return root;
}

export {
  parseSchema,
  INode,
  ITypeProperties,
  IOrderedChildren,
  parseComplexType,
  parseUnionType,
  parseArrayType,
  parseEnumType,
  parseMapType,
  checkForLogicalType,
};
