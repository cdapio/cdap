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

/**
 * This is purely a playground to play around and get an understanding on the
 * schema types. Not used anywhere.
 */
import {
  IArrayField,
  IArrayFieldNullable,
  IUnionField,
  IEnumField,
  IRecordField,
  IMapFieldBase,
  IMapField,
  IComplexType,
  ISimpleType,
  IComplexTypeFieldNullable,
} from 'components/AbstractWidget/SchemaEditor/SchemaTypes';
import { AvroSchemaTypesEnum } from 'components/AbstractWidget/SchemaEditor/SchemaConstants';

const mapfield: IMapField = {
  name: 'map1',
  type: {
    type: AvroSchemaTypesEnum.MAP,
    keys: AvroSchemaTypesEnum.STRING,
    values: AvroSchemaTypesEnum.STRING,
  },
};
// tslint:disable-next-line: no-console
console.log(mapfield.type.keys);

const arrayField: IArrayField = {
  name: 'arr',
  type: {
    type: AvroSchemaTypesEnum.ARRAY,
    items: AvroSchemaTypesEnum.STRING,
  },
};
// tslint:disable-next-line: no-console
console.log(!Array.isArray(arrayField.type) ? arrayField.type.items : arrayField.type[0]);

const unionField: IUnionField = {
  name: 'something',
  type: [AvroSchemaTypesEnum.LONG, AvroSchemaTypesEnum.STRING],
};
// tslint:disable-next-line: no-console
console.log(unionField.type[1]);

const enumField: IEnumField = {
  name: 'enum1',
  type: {
    type: AvroSchemaTypesEnum.ENUM,
    symbols: ['something', 'somethingelse', 'nothing', 'maybesomething'],
  },
};
// tslint:disable-next-line: no-console
// tslint:disable-next-line: no-console
console.log(enumField.type.symbols);

const recordField: IRecordField = {
  type: AvroSchemaTypesEnum.RECORD,
  name: 'record1',
  fields: [
    {
      name: 'name',
      type: AvroSchemaTypesEnum.STRING,
    },
    {
      name: 'email',
      type: AvroSchemaTypesEnum.STRING,
    },
  ],
};
// tslint:disable-next-line: no-console
// tslint:disable-next-line: no-console
console.log(recordField.fields);

const complexArrField2: IArrayFieldNullable = {
  name: 'arr1',
  type: [
    {
      type: AvroSchemaTypesEnum.ARRAY,
      items: AvroSchemaTypesEnum.STRING,
    },
    'null',
  ],
};
// tslint:disable-next-line: no-console
console.log(
  Array.isArray(complexArrField2.type)
    ? complexArrField2.type.find((t) => t !== 'null' && typeof t.items !== 'undefined')
    : complexArrField2.type
);

const isNullable = (type) => Array.isArray(type) && type.find((t) => t === 'null');
const complexArrayField: IArrayFieldNullable = {
  name: 'complexArray',
  type: [
    {
      type: AvroSchemaTypesEnum.ARRAY,
      items: [
        {
          type: AvroSchemaTypesEnum.RECORD,
          name: 'ad5bddf76ef2743218d79d3905f0f8e4f',
          fields: [
            {
              name: 'name',
              type: AvroSchemaTypesEnum.STRING,
            },
            {
              name: 'email',
              type: AvroSchemaTypesEnum.STRING,
            },
          ],
        },
        'null',
      ],
    },
    'null',
  ],
};

if (Array.isArray(complexArrayField.type)) {
  const a1 = complexArrayField.type.find(
    (t) => t !== 'null' && t.type === AvroSchemaTypesEnum.ARRAY
  );
  // tslint:disable-next-line: no-console
  console.log(a1 !== 'null' && a1.items);
  if (isNullable(complexArrayField.type)) {
    // tslint:disable-next-line: no-console
    console.log('nullable is true');
  }
}

const complexUnionField: IUnionField = {
  name: 'something',
  type: [
    AvroSchemaTypesEnum.LONG,
    {
      type: AvroSchemaTypesEnum.MAP,
      keys: {
        type: AvroSchemaTypesEnum.RECORD,
        name: 'a64d56b7343854e81801874b77b536802',
        fields: [
          {
            name: 'sdfsd',
            type: AvroSchemaTypesEnum.STRING,
          },
          {
            name: 'sdfsdsdfsdf',
            type: AvroSchemaTypesEnum.STRING,
          },
        ],
      },
      values: AvroSchemaTypesEnum.STRING,
    },
    {
      type: AvroSchemaTypesEnum.RECORD,
      name: 'record1',
      fields: [
        {
          name: 'name',
          type: AvroSchemaTypesEnum.STRING,
        },
        {
          name: 'email',
          type: AvroSchemaTypesEnum.STRING,
        },
      ],
    },
  ],
};
const isSimpleType = (type: ISimpleType | IComplexTypeFieldNullable) =>
  typeof type === 'string' &&
  [
    AvroSchemaTypesEnum.BOOLEAN,
    AvroSchemaTypesEnum.BYTES,
    AvroSchemaTypesEnum.DATE,
    AvroSchemaTypesEnum.DECIMAL,
    AvroSchemaTypesEnum.DOUBLE,
    AvroSchemaTypesEnum.FLOAT,
    AvroSchemaTypesEnum.INT,
    AvroSchemaTypesEnum.LONG,
    AvroSchemaTypesEnum.STRING,
    AvroSchemaTypesEnum.TIME,
  ].indexOf(type) !== -1;
if (Array.isArray(complexUnionField.type)) {
  const map1 = complexUnionField.type
    .filter((t) => typeof t !== 'string')
    .find(
      (t: IComplexType) => !Array.isArray(t.type) && t.type === AvroSchemaTypesEnum.MAP
    ) as IMapFieldBase;
  let fieldsInRecords;
  if (
    !Array.isArray(map1.keys) &&
    typeof map1.keys === 'object' &&
    map1.keys.type === AvroSchemaTypesEnum.RECORD
  ) {
    fieldsInRecords = (map1.keys as IRecordField).fields;
  }
  // tslint:disable-next-line: no-console
  console.log(map1, fieldsInRecords);
}
