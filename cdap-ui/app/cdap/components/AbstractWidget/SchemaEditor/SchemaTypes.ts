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

type IComplexTypeNames = 'array' | 'enum' | 'map' | 'record' | 'union';
type ISimpleType =
  | 'boolean'
  | 'bytes'
  | 'date'
  | 'decimal'
  | 'double'
  | 'float'
  | 'int'
  | 'long'
  | 'number'
  | 'string'
  | 'time';
type ISimpleTypeNullable = Array<ISimpleType | 'null'>;

type IComplexType = IArrayField | IEnumFieldBase | IMapFieldBase | IRecordField | IUnionField;
type IComplexTypeFieldNullable =
  | IArrayFieldNullable
  | IEnumFieldNullable
  | IMapFieldNullable
  | IRecordFieldNullable;

interface IFieldBaseType {
  name: string;
}

interface IEnumFieldBase {
  type: 'enum';
  symbols: string[];
}
interface IEnumField extends IFieldBaseType {
  type: IEnumFieldBase;
}
interface IEnumFieldNullable extends IFieldBaseType {
  type: Array<IEnumFieldBase | 'null'>;
}

interface IMapFieldBase {
  type: 'map';
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
  type: 'array';
  items: ISimpleType | ISimpleTypeNullable | IComplexType | IComplexTypeFieldNullable;
}

interface IArrayField extends IFieldBaseType {
  type: IArrayFieldBase;
}
interface IArrayFieldNullable extends IFieldBaseType {
  type: Array<IArrayFieldBase | 'null'>;
}

interface IFieldType extends IFieldBaseType {
  type: ISimpleType | IComplexType;
}

interface IFieldTypeNullable extends IFieldBaseType {
  type: ISimpleTypeNullable | IComplexTypeFieldNullable;
}

interface IRecordField extends IFieldBaseType {
  type: 'record';
  fields: Array<IFieldType | IFieldTypeNullable>;
}
type IRecordFieldNullable = Array<IRecordField | 'null'>;

interface IUnionField extends IFieldBaseType {
  type: Array<ISimpleType | IComplexType>;
}

interface ISchemaType {
  name: string;
  schema: IRecordField;
}

interface IFlattenRowSimpleType {
  name: string;
  type: ISimpleType;
  nullable: boolean;
}

interface IFlattenRowComplexType {
  name: string;
  type: IComplexType;
  nullable: boolean;
  children: number;
  collapsed: boolean;
}

export {
  ISimpleType,
  IComplexTypeNames,
  ISimpleTypeNullable,
  IComplexTypeFieldNullable,
  IComplexType,
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
  IFieldBaseType,
  ISchemaType,
  IFlattenRowSimpleType,
  IFlattenRowComplexType,
};
