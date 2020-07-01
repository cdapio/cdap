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

import { ISchemaType } from './SchemaTypes';

/**
 * Defines all the defaults we use for the schema.
 */
const logicalTypes = ['time', 'timestamp', 'decimal', 'date'];
const defaultPrecision = 32;
const defaultScale = 3;
const defaultDecimalTypeProperties = {
  type: 'bytes',
  logicalType: 'decimal',
  precision: defaultPrecision,
  scale: defaultScale,
};
const defaultTimeTypeProperties = {
  type: 'long',
  logicalType: 'time-micros',
};
const defaultTimeStampTypeProperties = {
  type: 'long',
  logicalType: 'timestamp-micros',
};
const defaultDateTypeProperties = {
  type: 'int',
  logicalType: 'date',
};

const defaultArrayType = {
  type: 'array',
  items: 'string',
};
const defaultEnumType = {
  type: 'enum',
  symbols: [''],
};
const defaultMapType = {
  type: 'map',
  keys: 'string',
  values: 'string',
};
const defaultRecordType = {
  name: 'etlSchemaBody',
  type: 'record',
  fields: [
    {
      name: '',
      type: 'string',
    },
  ],
};
const defaultFieldType = {
  name: '',
  type: 'string',
};
const defaultUnionType = ['string'];

const schemaTypes = [
  'array',
  'boolean',
  'bytes',
  'double',
  'enum',
  'float',
  'int',
  'long',
  'map',
  'record',
  'string',
  'union',
].concat(logicalTypes);

const logicalTypeToSimpleTypeMap = {
  'time-micros': 'time',
  'timestamp-micros': 'timestamp',
  date: 'date',
  decimal: 'decimal',
};

const INDENTATION_SPACING = 15;

const getDefaultEmptyAvroSchema = (): ISchemaType => {
  return {
    name: 'etlSchemaBody',
    schema: {
      name: 'etlSchemaBody',
      type: 'record',
      fields: [
        {
          name: '',
          type: 'string',
        },
      ],
    },
  };
};

enum InternalTypesEnum {
  SCHEMA = 'schema',
  RECORD_SIMPLE_TYPE = 'record-field-simple-type',
  RECORD_COMPLEX_TYPE_ROOT = 'record-field-complex-type-root',
  ARRAY_SIMPLE_TYPE = 'array-simple-type',
  ARRAY_COMPLEX_TYPE = 'array-complex-type',
  ARRAY_COMPLEX_TYPE_ROOT = 'array-complex-type-root',
  ENUM_SYMBOL = 'enum-symbol',
  MAP_KEYS_COMPLEX_TYPE_ROOT = 'map-keys-complex-type-root',
  MAP_KEYS_SIMPLE_TYPE = 'map-keys-simple-type',
  MAP_VALUES_COMPLEX_TYPE_ROOT = 'map-values-complex-type-root',
  MAP_VALUES_SIMPLE_TYPE = 'map-values-simple-type',
  UNION_SIMPLE_TYPE = 'union-simple-type',
  UNION_COMPLEX_TYPE_ROOT = 'union-complex-type-root',
}

enum OperationTypesEnum {
  UPDATE = 'update',
  ADD = 'add',
  REMOVE = 'remove',
  COLLAPSE = 'collapse',
}

export {
  schemaTypes,
  INDENTATION_SPACING,
  logicalTypes,
  logicalTypeToSimpleTypeMap,
  defaultPrecision,
  defaultScale,
  defaultTimeStampTypeProperties,
  defaultDecimalTypeProperties,
  defaultTimeTypeProperties,
  defaultDateTypeProperties,
  defaultArrayType,
  defaultEnumType,
  defaultMapType,
  defaultRecordType,
  defaultUnionType,
  defaultFieldType,
  getDefaultEmptyAvroSchema,
  InternalTypesEnum,
  OperationTypesEnum,
};
