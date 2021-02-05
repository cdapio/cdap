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

import { AvroSchemaTypesEnum } from 'components/AbstractWidget/SchemaEditor/SchemaConstants';
/**
 * Contains types used in parsing an avro schema.
 * TODO: This is a work in progress. We don't use these types yet fully
 * in the schema parser yet.
 */
type IComplexTypeNames =
  | AvroSchemaTypesEnum.ARRAY
  | AvroSchemaTypesEnum.ENUM
  | AvroSchemaTypesEnum.MAP
  | AvroSchemaTypesEnum.RECORD
  | AvroSchemaTypesEnum.UNION;
type ISimpleType =
  | AvroSchemaTypesEnum.BOOLEAN
  | AvroSchemaTypesEnum.BYTES
  | AvroSchemaTypesEnum.DATE
  | AvroSchemaTypesEnum.DATETIME
  | AvroSchemaTypesEnum.DECIMAL
  | AvroSchemaTypesEnum.DOUBLE
  | AvroSchemaTypesEnum.FLOAT
  | AvroSchemaTypesEnum.INT
  | AvroSchemaTypesEnum.LONG
  | AvroSchemaTypesEnum.STRING
  | AvroSchemaTypesEnum.TIME
  | AvroSchemaTypesEnum.TIMESTAMP;
type ILogicalTypeNames =
  | AvroSchemaTypesEnum.TIMESTAMPMICROS
  | AvroSchemaTypesEnum.DATE
  | AvroSchemaTypesEnum.DATETIME
  | AvroSchemaTypesEnum.TIMEMICROS
  | AvroSchemaTypesEnum.DECIMAL;

type IDisplayType = ISimpleType | IComplexTypeNames;

type ISimpleTypeNullable = Array<ISimpleType | 'null'>;

type IComplexType =
  | IArrayFieldBase
  | IEnumFieldBase
  | IMapFieldBase
  | IRecordField
  | IUnionField
  | ILogicalTypeBase;
type IComplexTypeNullable =
  | Array<IArrayFieldBase | 'null'>
  | Array<IEnumFieldBase | 'null'>
  | Array<IMapFieldBase | 'null'>
  | Array<IRecordField | 'null'>;

type IComplexTypeFieldNullable =
  | IArrayFieldNullable
  | IEnumFieldNullable
  | IMapFieldNullable
  | IRecordFieldNullable;

interface IFieldBaseType {
  name: string;
}

interface IEnumFieldBase {
  type: AvroSchemaTypesEnum.ENUM;
  symbols: string[];
  doc?: string;
  aliases?: string[];
}
interface IEnumField extends IFieldBaseType {
  type: IEnumFieldBase;
}
interface IEnumFieldNullable extends IFieldBaseType {
  type: Array<IEnumFieldBase | 'null'>;
}

interface IMapFieldBase {
  type: AvroSchemaTypesEnum.MAP;
  keys: ISimpleType | ISimpleTypeNullable | IComplexType | IComplexTypeFieldNullable;
  values: ISimpleType | ISimpleTypeNullable | IComplexType | IComplexTypeFieldNullable;
}
interface IMapField extends IFieldBaseType {
  type: IMapFieldBase;
}
interface IMapFieldNullable extends IFieldBaseType {
  type: Array<IMapFieldBase | 'null'>;
}

interface IArrayFieldBase {
  type: AvroSchemaTypesEnum.ARRAY;
  items: ISimpleType | ISimpleTypeNullable | IComplexType | IComplexTypeFieldNullable;
}

interface IArrayField extends IFieldBaseType {
  type: IArrayFieldBase;
}
interface IArrayFieldNullable extends IFieldBaseType {
  type: Array<IArrayFieldBase | 'null'>;
}

interface ILogicalTypeBase {
  type: ISimpleType;
  logicalType: ILogicalTypeNames;
  precision?: number;
  scale?: number;
}

type ILogicalTypeNullable = Array<ILogicalTypeBase | 'null'>;

interface ILogicalType extends IFieldBaseType {
  type: ILogicalTypeBase;
}

interface ILogicalTypeFieldNullable extends IFieldBaseType {
  type: Array<ILogicalTypeBase | 'null'>;
}

interface IFieldType extends IFieldBaseType {
  type: ISimpleType | IComplexType | ILogicalType;
  doc?: string;
  aliases?: string[];
}

interface IFieldTypeNullable extends IFieldBaseType {
  type: ISimpleTypeNullable | IComplexTypeNullable | ILogicalTypeNullable;
  doc?: string;
  aliases?: string[];
}

interface IRecordField extends IFieldBaseType {
  type: AvroSchemaTypesEnum.RECORD;
  fields: Array<IFieldType | IFieldTypeNullable>;
  doc?: string;
  aliases?: string[];
}
type IRecordFieldNullable = Array<IRecordField | 'null'>;

interface IUnionField extends IFieldBaseType {
  type: Array<ISimpleType | IComplexType>;
}

interface ISchemaType {
  name: string;
  schema: IRecordField;
}

export {
  ISimpleType,
  IComplexTypeNames,
  ILogicalTypeNames,
  ILogicalType,
  ILogicalTypeFieldNullable,
  ILogicalTypeNullable,
  IDisplayType,
  ISimpleTypeNullable,
  IComplexTypeNullable,
  IComplexTypeFieldNullable,
  IComplexType,
  IEnumFieldBase,
  IEnumField,
  IEnumFieldNullable,
  IMapFieldBase,
  IMapField,
  IMapFieldNullable,
  IArrayFieldBase,
  IArrayField,
  IArrayFieldNullable,
  IRecordField,
  IRecordFieldNullable,
  IUnionField,
  IFieldType,
  IFieldTypeNullable,
  IFieldBaseType,
  ISchemaType,
  ILogicalTypeBase,
};
