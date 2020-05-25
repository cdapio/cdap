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
  getSimpleType,
  getComplexType,
} from 'components/AbstractWidget/SchemaEditor/SchemaHelpers';

const flattenUnionType = (complexType, parent) => {
  if (!Array.isArray(complexType)) {
    return [];
  }
  const result = [];
  for (const type of complexType) {
    if (isComplexType(type)) {
      result.push(...flattenSubTree(type, parent));
    } else {
      result.push({
        name: 'union-type',
        type,
        parent,
        displayType: 'union-child',
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
  const { keys, values } = complexType;
  if (!isComplexType(keys)) {
    result.push({
      name: 'map-keys-type',
      displayType: keys,
      type: keys,
      parent,
      nullable: isNullable(keys),
    });
  } else {
    const mapKeysId = uuidV4();
    result.push({
      name: mapKeysId,
      displayType: 'map-keys-type',
      parent,
      nullable: isNullable(keys),
    });
    result.push(...flattenSubTree(keys, parent.concat([mapKeysId])));
  }
  if (!isComplexType(values)) {
    result.push({
      name: 'map-values-type',
      displayType: values,
      type: values,
      parent,
      nullable: isNullable(values),
    });
  } else {
    const mapValuesId = uuidV4();
    result.push({
      name: mapValuesId,
      displayType: 'map-values-type',
      parent,
      nullable: isNullable(values),
    });
    result.push(...flattenSubTree(values, parent.concat([mapValuesId])));
  }
  return result;
};

const flattenSubTree = (complexType, parent) => {
  const type = getComplexType(complexType);
  switch (type) {
    case 'union':
      return flattenUnionType(complexType, parent);
    case 'map':
      return flattenMapType(complexType, parent);
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
      fieldObj.type = getSimpleType(field.type);
      fieldObj.displayType = fieldObj.type;
      result.push(fieldObj);
    } else {
      fieldObj.type = getComplexType(field.type);
      fieldObj.displayType = fieldObj.type;
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
    pre: {
      margin: '10px 10px 0 20px',
      background: theme.palette.grey[600],
      padding: '10px',
      borderRadius: '4px',
      border: `1px solid ${theme.palette.grey[300]}`,
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
    <React.Fragment>
      <h1>Schema Editor</h1>
      <h3> Schema Name: {schema[0].schema.name}</h3>
      <h3> Fields: </h3>
      {flattenSchema(schema[0]).map((field) => {
        const spacing = field.parent && Array.isArray(field.parent) ? field.parent.length * 10 : 0;
        return (
          <div
            key={field.name}
            style={{
              marginLeft: `${spacing}px`,
              height: 'auto',
              borderLeft: '1px solid',
              padding: '10px',
              display: 'grid',
              gridTemplateColumns: `calc(200px - ${spacing + 10}px) 200px 200px 200px`,
            }}
          >
            <div>{field.name}</div>
            <div>{field.displayType}</div>
            <div>{field.nullable ? 'nullable' : 'not-nullable'}</div>
          </div>
        );
      })}
      &nbsp; &nbsp;
      <pre>{JSON.stringify(flattenSchema(schema[0]), null, 2)}</pre>
    </React.Fragment>
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
