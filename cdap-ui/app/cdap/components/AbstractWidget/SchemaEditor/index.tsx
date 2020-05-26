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
import { ISchemaType } from 'components/AbstractWidget/SchemaEditor/SchemaTypes';
import { flattenSchema } from 'components/AbstractWidget/SchemaEditor/schemaParser';
import If from 'components/If';

const styles = () => {
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
              key={field.id}
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
