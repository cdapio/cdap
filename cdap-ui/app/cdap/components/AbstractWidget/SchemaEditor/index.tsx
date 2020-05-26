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

import * as React from 'react';
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';
import ThemeWrapper from 'components/ThemeWrapper';
import flatten from 'lodash/flattenDeep';
import isObject from 'lodash/isObject';
import { ISchemaType, IFieldType } from 'components/AbstractWidget/SchemaEditor/SchemaTypes';
import uuidV4 from 'uuid/v4';
import {
  isNullable,
  isComplexType,
  getNonNullableType,
  getComplexTypeName,
} from 'components/AbstractWidget/SchemaEditor/SchemaHelpers';
import If from 'components/If';

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
        name,
        type: complexTypeName,
        parent,
        nullable: isNullable(subType),
      });
      result.push(...flattenSubTree(subType, parent.concat([name])));
    } else {
      result.push({
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
      name: mapKeysId,
      type: keys,
      parent,
      nullable: isNullable(keys),
    });
  } else {
    const complexTypeName = getComplexTypeName(keys);
    result.push({
      name: mapKeysId,
      type: complexTypeName,
      parent,
      nullable: isNullable(keys),
    });
    result.push(...flattenSubTree(keys, parent.concat([mapKeysId])));
  }
  if (!isComplexType(values)) {
    result.push({
      name: mapValuesId,
      type: values,
      parent,
      nullable: isNullable(values),
    });
  } else {
    const complexTypeName = getComplexTypeName(values);
    result.push({
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
  for (const symbol of symbols) {
    result.push({
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
  if (!isComplexType(items)) {
    result.push({
      type: getNonNullableType(items),
      parent,
      nullable,
    });
  } else {
    const complexTypeName = getComplexTypeName(items);
    const itemsComplexId = uuidV4();
    result.push({
      type: complexTypeName,
      name: itemsComplexId,
      nullable,
      parent,
    });
    result.push(...flattenSubTree(complexType, parent.concat([itemsComplexId])));
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
      name: field.name,
      nullable,
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
const flattenSchema = (s: ISchemaType, parent = ['root']) => {
  const schema = s.schema;
  const op = [];
  if (schema.fields) {
    op.push(...flattenFields(schema.fields, [schema.name]));
  }
  return op;
};
const styles = (theme) => {
  return {
    container: {
      height: 'auto',
    },
    schemaContainer: {
      width: '500px',
      margin: '20px',
      height: '2000px',
      '& >div': {
        width: '100%',
        borderBottom: '1px solid',
        height: 'auto',
        borderLeft: '1px solid',
        padding: '10px',
        display: 'grid',
      },
    },
  };
};

interface ISchemaEditorProps extends WithStyles<typeof styles> {
  schema: ISchemaType;
}

function SchemaEditor({ schema, classes }: ISchemaEditorProps) {
  if (!schema) {
    return null;
  }
  return (
    <div className={classes.container}>
      <h1>Schema Editor</h1>
      <h3> Schema Name: {schema[0].schema.name}</h3>
      <h3> Fields: </h3>
      <div className={classes.schemaContainer}>
        {flattenSchema(schema[0]).map((field) => {
          const spacing =
            field.parent && Array.isArray(field.parent) ? field.parent.length * 10 : 0;
          return (
            <div
              key={field.name}
              style={{
                marginLeft: `${spacing}px`,
                gridTemplateColumns: `calc(33% - ${spacing + 10}px) 34% 33%`,
              }}
            >
              <If condition={field.name}>
                <div>{field.name}</div>
              </If>
              <If condition={field.symbol}>{field.symbol}</If>
              <div>{field.type}</div>
              <div>{field.nullable ? 'nullable' : 'not-nullable'}</div>
            </div>
          );
        })}
      </div>
      <pre>{JSON.stringify(flattenSchema(schema[0]), null, 2)}</pre>
    </div>
  );
}

const StyledDemo = withStyles(styles)(SchemaEditor);
export default function SchemaEditorWrapper(props) {
  return (
    <ThemeWrapper>
      <StyledDemo {...props} />
    </ThemeWrapper>
  );
}
