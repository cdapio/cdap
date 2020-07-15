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
  SchemaManager,
  ISchemaManager,
} from 'components/AbstractWidget/SchemaEditor/Context/SchemaManager';
import { INode } from 'components/AbstractWidget/SchemaEditor/Context/SchemaParser';
import {
  OperationTypesEnum,
  getDefaultEmptyAvroSchema,
} from 'components/AbstractWidget/SchemaEditor/SchemaConstants';
import {
  schemaWithMap,
  schemaWithName,
  schemaWithModifiedType,
  simpleSchema,
  simpleSchema2,
  largeSchema,
  largeSchema2,
} from './schemas';
import { ISchemaType } from 'components/AbstractWidget/SchemaEditor/SchemaTypes';

const childCountInTree = (tree) => {
  if (!tree || (tree && !tree.children)) {
    return 0;
  }
  return Object.keys(tree.children).filter((child) => child !== 'order').length;
};

const getNthFieldIdInFlatSchema = (schemaManagerInstance: ISchemaManager, n) => {
  const list = schemaManagerInstance.getFlatSchema();
  const { id, ancestors } = list[n];
  const fieldId = { id, ancestors };
  return { fieldId, field: list[n] };
};

describe('Unit tests for Schema Manager', () => {
  describe('Basic operations should work', () => {
    let schema;
    beforeEach(() => {
      schema = SchemaManager().getInstance();
    });

    it('Should initialize to a default empty schema', () => {
      expect(schema).not.toBeNull();
      const tree = schema.getSchemaTree();
      const list = schema.getFlatSchema();
      expect(childCountInTree(tree)).toBe(1);
      expect(list.length).toBe(2); // top level schema + first empty record field.
      expect(list[1].name).toBe('');
    });

    it('Should change "name" of a record field', () => {
      const list = schema.getFlatSchema();
      const { id, ancestors } = list[1];
      const fieldId = { id, ancestors };
      schema.onChange(fieldId, {
        property: 'name',
        value: 'new_field',
        type: 'update',
      });
      expect(schema.getFlatSchema().length).toBe(2);
      expect(list[1].name).toBe('new_field');
      expect(schema.getAvroSchema()).toEqual(schemaWithName);
    });

    it('Should change the "type" of a record field', () => {
      const { fieldId } = getNthFieldIdInFlatSchema(schema, 1);
      schema.onChange(fieldId, {
        property: 'name',
        value: 'new_field',
        type: OperationTypesEnum.UPDATE,
      });
      schema.onChange(fieldId, {
        property: 'type',
        value: 'boolean',
        type: OperationTypesEnum.UPDATE,
      });
      expect(schema.getFlatSchema()[1].type).toBe('boolean');
      expect(schema.getAvroSchema()).toEqual(schemaWithModifiedType);
    });

    it('Should add a new record field', () => {
      const { fieldId } = getNthFieldIdInFlatSchema(schema, 1);
      schema.onChange(fieldId, { type: OperationTypesEnum.ADD });
      const flatSchema = schema.getFlatSchema();
      const newField = flatSchema.find((field) => field.id === fieldId);
      expect(flatSchema.length).toBe(3);
      expect(newField).not.toBe(null);
    });

    it('Should remove a record field', () => {
      const { fieldId } = getNthFieldIdInFlatSchema(schema, 1);
      schema.onChange(fieldId, { type: OperationTypesEnum.ADD });
      schema.onChange(fieldId, { type: OperationTypesEnum.REMOVE });
      const flatSchema = schema.getFlatSchema();
      const newField = flatSchema.find((field) => field.id === fieldId);
      expect(flatSchema.length).toBe(2);
      expect(newField).toBe(undefined);
    });

    it('Should change simple type to complex type', () => {
      const { fieldId } = getNthFieldIdInFlatSchema(schema, 1);
      schema.onChange(fieldId, {
        property: 'name',
        value: 'new_field',
        type: 'update',
      });
      schema.onChange(fieldId, {
        property: 'type',
        value: 'map',
        type: OperationTypesEnum.UPDATE,
      });
      expect(schema.getFlatSchema().length).toBe(4);
      const mapRoot = schema.getSchemaTree().children;
      const mapSubTree = mapRoot[mapRoot.order[0]];
      expect(childCountInTree(mapSubTree)).toBe(2);
      expect(schema.getAvroSchema()).toEqual(schemaWithMap);
    });

    it('Should change complex type to simple type', () => {
      const { fieldId } = getNthFieldIdInFlatSchema(schema, 1);
      schema.onChange(fieldId, {
        property: 'name',
        value: 'new_field',
        type: 'update',
      });
      schema.onChange(fieldId, {
        property: 'type',
        value: 'map',
        type: OperationTypesEnum.UPDATE,
      });
      expect(schema.getFlatSchema().length).toBe(4);
      schema.onChange(fieldId, {
        property: 'type',
        value: 'boolean',
        type: OperationTypesEnum.UPDATE,
      });
      expect(schema.getFlatSchema()[1].type).toBe('boolean');
      expect(schema.getFlatSchema().length).toBe(2);
    });
  });

  describe('Should handle simple schemas', () => {
    it('Should parse and manage simple schema', () => {
      const schema = SchemaManager(simpleSchema as ISchemaType).getInstance();
      expect(schema.getSchemaTree()).not.toBeNull();
      expect(schema.getFlatSchema()).not.toBeNull();
      expect(schema.getFlatSchema().length).toBe(6);
      const { field } = getNthFieldIdInFlatSchema(schema, 1);
      expect(field.collapsed).toBe(true);
    });

    it('Should parse and manage simple schema (2)', () => {
      const schema = SchemaManager(simpleSchema2 as ISchemaType).getInstance();
      expect(schema.getFlatSchema().length).toBe(5);
      expect(childCountInTree(schema.getSchemaTree())).toBe(4);
    });

    it('Should change type in simple schema', () => {
      const schema = SchemaManager(simpleSchema2 as ISchemaType).getInstance();
      const { fieldId } = getNthFieldIdInFlatSchema(schema, 1);
      schema.onChange(fieldId, {
        property: 'type',
        value: 'map',
        type: OperationTypesEnum.UPDATE,
      });
      expect(schema.getFlatSchema().length).toBe(7); // additional 2 rows for key and value types
      const tree = schema.getSchemaTree();
      const mapRoot = tree.children[tree.children.order[0]];
      expect(childCountInTree(mapRoot)).toBe(2);
    });

    it('Should change nullable for an existing field in simple schema', () => {
      const schema = SchemaManager(simpleSchema2 as ISchemaType).getInstance();
      const { fieldId } = getNthFieldIdInFlatSchema(schema, 1);
      schema.onChange(fieldId, {
        property: 'nullable',
        value: true,
        type: OperationTypesEnum.UPDATE,
      });
      const { field } = getNthFieldIdInFlatSchema(schema, 1);
      expect(field.nullable).toBe(true);
      const avroSchema = schema.getAvroSchema();
      expect(avroSchema.schema.fields[0].type).toBeInstanceOf(Array);
      expect((avroSchema.schema.fields[0].type as []).find((t) => t === 'null')).toBe('null');
    });

    it('Should delete existing field from simple schema', () => {
      const schema = SchemaManager(simpleSchema2 as ISchemaType).getInstance();
      const { fieldId } = getNthFieldIdInFlatSchema(schema, 1);
      schema.onChange(fieldId, { type: OperationTypesEnum.REMOVE });
      expect(childCountInTree(schema.getSchemaTree())).toBe(3);
    });
  });

  describe('Should handle large schemas', () => {
    it('Should parse and manage large schema', () => {
      const schema = SchemaManager(largeSchema).getInstance();
      expect(schema.getFlatSchema().length).toBe(662);
      const { field } = getNthFieldIdInFlatSchema(schema, 1);
      expect(field.collapsed).toBe(true);
    });

    it('Should parse and manage large schema (2)', () => {
      const schema = SchemaManager(largeSchema2).getInstance();
      expect(schema.getFlatSchema().length).toBe(10001);
      expect(childCountInTree(schema.getSchemaTree())).toBe(10000);
    });

    it('Should change type in large schema', () => {
      const schema = SchemaManager(largeSchema).getInstance();
      const { fieldId } = getNthFieldIdInFlatSchema(schema, 2);
      schema.onChange(fieldId, {
        property: 'type',
        value: 'map',
        type: OperationTypesEnum.UPDATE,
      });
      expect(schema.getFlatSchema().length).toBe(664);
      const tree = schema.getSchemaTree();
      const firstChild: INode = tree.children[tree.children.order[0]];
      const childrenOfFirstChild = firstChild.children;
      expect((childrenOfFirstChild.order as string[]).length).toBe(12);
    });

    it('Should change nullable for an existing field in large schema', () => {
      const schema = SchemaManager(largeSchema).getInstance();
      const { fieldId } = getNthFieldIdInFlatSchema(schema, 1);
      schema.onChange(fieldId, {
        property: 'nullable',
        value: true,
        type: OperationTypesEnum.UPDATE,
      });
      const { field } = getNthFieldIdInFlatSchema(schema, 1);
      expect(field.nullable).toBe(true);
      const avroSchema = schema.getAvroSchema();
      expect(avroSchema.schema.fields[0].type).toBeInstanceOf(Array);
      expect((avroSchema.schema.fields[0].type as []).find((t) => t === 'null')).toBe('null');
    });

    it('Should delete existing field in large schema', () => {
      const schema = SchemaManager(largeSchema).getInstance();
      const { fieldId } = getNthFieldIdInFlatSchema(schema, 1);
      schema.onChange(fieldId, {
        type: OperationTypesEnum.REMOVE,
      });
      expect(schema.getFlatSchema().length).toBe(2);
      const defaultSchema = getDefaultEmptyAvroSchema();
      defaultSchema.schema.fields = [];
      expect(schema.getAvroSchema()).toEqual(defaultSchema);
    });
  });
});
