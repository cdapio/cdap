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

enum AvroSchemaTypesEnum {
  ARRAY = 'array',
  BOOLEAN = 'boolean',
  BYTES = 'bytes',
  DATE = 'date',
  DATETIME = 'datetime',
  DECIMAL = 'decimal',
  DOUBLE = 'double',
  ENUM = 'enum',
  FLOAT = 'float',
  INT = 'int',
  LONG = 'long',
  MAP = 'map',
  RECORD = 'record',
  STRING = 'string',
  TIME = 'time',
  TIMESTAMP = 'timestamp',
  UNION = 'union',
  TIMESTAMPMICROS = 'timestamp-micros',
  TIMEMICROS = 'time-micros',
}

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

/**
 * Defines all the defaults we use for the schema.
 */
const logicalTypes = [
  AvroSchemaTypesEnum.TIME,
  AvroSchemaTypesEnum.TIMESTAMP,
  AvroSchemaTypesEnum.DECIMAL,
  AvroSchemaTypesEnum.DATE,
  AvroSchemaTypesEnum.DATETIME,
];
const defaultPrecision = 32;
const defaultScale = 3;
const defaultDecimalTypeProperties = {
  type: AvroSchemaTypesEnum.BYTES,
  logicalType: AvroSchemaTypesEnum.DECIMAL,
  precision: defaultPrecision,
  scale: defaultScale,
};
const defaultTimeTypeProperties = {
  type: AvroSchemaTypesEnum.LONG,
  logicalType: AvroSchemaTypesEnum.TIMEMICROS,
};
const defaultTimeStampTypeProperties = {
  type: AvroSchemaTypesEnum.LONG,
  logicalType: AvroSchemaTypesEnum.TIMESTAMPMICROS,
};
const defaultDateTypeProperties = {
  type: AvroSchemaTypesEnum.INT,
  logicalType: AvroSchemaTypesEnum.DATE,
};
const defaultDateTimeTypeProperties = {
  type: AvroSchemaTypesEnum.STRING,
  logicalType: AvroSchemaTypesEnum.DATETIME,
};

const defaultArrayType = {
  type: AvroSchemaTypesEnum.ARRAY,
  items: AvroSchemaTypesEnum.STRING,
};
const defaultEnumType = {
  type: AvroSchemaTypesEnum.ENUM,
  symbols: [''],
};
const defaultMapType = {
  type: AvroSchemaTypesEnum.MAP,
  keys: AvroSchemaTypesEnum.STRING,
  values: AvroSchemaTypesEnum.STRING,
};
const defaultRecordType = {
  name: 'etlSchemaBody',
  type: AvroSchemaTypesEnum.RECORD,
  fields: [
    {
      name: '',
      type: AvroSchemaTypesEnum.STRING,
    },
  ],
};
const defaultFieldType = {
  name: '',
  type: AvroSchemaTypesEnum.STRING,
};
const defaultUnionType = [AvroSchemaTypesEnum.STRING];

const schemaTypes = [
  AvroSchemaTypesEnum.ARRAY,
  AvroSchemaTypesEnum.BOOLEAN,
  AvroSchemaTypesEnum.BYTES,
  AvroSchemaTypesEnum.DOUBLE,
  AvroSchemaTypesEnum.ENUM,
  AvroSchemaTypesEnum.FLOAT,
  AvroSchemaTypesEnum.INT,
  AvroSchemaTypesEnum.LONG,
  AvroSchemaTypesEnum.MAP,
  AvroSchemaTypesEnum.RECORD,
  AvroSchemaTypesEnum.STRING,
  AvroSchemaTypesEnum.UNION,
].concat(logicalTypes);

const logicalTypeToSimpleTypeMap = {
  'time-micros': AvroSchemaTypesEnum.TIME,
  'timestamp-micros': AvroSchemaTypesEnum.TIMESTAMP,
  date: AvroSchemaTypesEnum.DATE,
  decimal: AvroSchemaTypesEnum.DECIMAL,
  datetime: AvroSchemaTypesEnum.DATETIME,
};

const INDENTATION_SPACING = 10;

const getDefaultEmptyAvroSchema = (): ISchemaType => {
  return {
    name: 'etlSchemaBody',
    schema: {
      name: 'etlSchemaBody',
      type: AvroSchemaTypesEnum.RECORD,
      fields: [
        {
          name: '',
          type: AvroSchemaTypesEnum.STRING,
        },
      ],
    },
  };
};

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
  defaultDateTimeTypeProperties,
  defaultArrayType,
  defaultEnumType,
  defaultMapType,
  defaultRecordType,
  defaultUnionType,
  defaultFieldType,
  getDefaultEmptyAvroSchema,
  AvroSchemaTypesEnum,
  InternalTypesEnum,
  OperationTypesEnum,
};
