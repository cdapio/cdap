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
import withStyles, { WithStyles, StyleRules } from '@material-ui/styles/withStyles';
import ThemeWrapper from 'components/ThemeWrapper';
import {
  SchemaManager,
  ISchemaManager,
  IOnChangeReturnType,
} from 'components/AbstractWidget/SchemaEditor/Context/SchemaManager';
import { ISchemaType, IFieldType } from 'components/AbstractWidget/SchemaEditor/SchemaTypes';
import {
  IFlattenRowType,
  IFieldIdentifier,
  IOnChangePayload,
} from 'components/AbstractWidget/SchemaEditor/EditorTypes';
import { FieldsList, FieldsListBase } from 'components/AbstractWidget/SchemaEditor/FieldsList';
import {
  SchemaValidatorConsumer,
  SchemaValidatorProvider,
} from 'components/AbstractWidget/SchemaEditor/SchemaValidator';
import { dumbestClone } from 'services/helpers';
import PropTypes from 'prop-types';
import {
  getDefaultEmptyAvroSchema,
  OperationTypesEnum,
} from 'components/AbstractWidget/SchemaEditor/SchemaConstants';
import { INode } from 'components/AbstractWidget/SchemaEditor/Context/SchemaParser';
import isEqual from 'lodash/isEqual';

const styles = (theme): StyleRules => {
  return {
    schemaContainer: {
      width: '100%',
      height: '100%',
    },
  };
};

interface ISchemaEditorProps extends WithStyles<typeof styles> {
  visibleRows?: number;
  schema: ISchemaType;
  disabled?: boolean;
  onChange: (props: {
    tree: INode;
    flat: IFlattenRowType[];
    avroSchema: ISchemaType;
  }) => IOnChangeReturnType;
  errors?: Record<string, string>;
}

interface ISchemaEditorState {
  tree: INode;
  flat: IFlattenRowType[];
  schemaRowCount: number;
  errors: Record<string, string>;
}

class SchemaEditorBase extends React.Component<ISchemaEditorProps, ISchemaEditorState> {
  private schema: ISchemaManager = null;
  private validate;
  constructor(props) {
    super(props);
    const { schema = getDefaultEmptyAvroSchema(), options } = props;
    this.schema = SchemaManager(schema, options).getInstance();
    this.state = {
      flat: dumbestClone(this.schema.getFlatSchema()),
      tree: dumbestClone(this.schema.getSchemaTree()),
      schemaRowCount: this.props.visibleRows,
      errors: null,
    };
  }
  public componentDidMount() {
    if (this.validate) {
      this.validate(this.schema.getFlatSchema()[1], this.schema.getSchemaTree());
    }
  }

  public componentWillReceiveProps(nextProps) {
    const { visibleRows, errors } = nextProps;
    const newState: Partial<ISchemaEditorState> = {};
    if (visibleRows !== this.state.schemaRowCount) {
      newState.schemaRowCount = visibleRows;
    }
    if (errors && !isEqual(errors, this.state.errors)) {
      const mapOfRowIdToError = {};
      const fieldNameToFieldMap: Record<string, IFlattenRowType> = {};
      this.schema.getFlatSchema().map((field) => {
        fieldNameToFieldMap[field.name] = field;
      });
      for (const fieldName of Object.keys(errors)) {
        const matchingField = fieldNameToFieldMap[fieldName];
        if (matchingField && typeof matchingField.id === 'string') {
          mapOfRowIdToError[matchingField.id] = errors[fieldName];
        }
      }
      newState.errors = mapOfRowIdToError;
    }
    if (Object.keys(newState).length) {
      this.setState(newState as ISchemaEditorState);
    }
  }
  public onChange = (validate, fieldId: IFieldIdentifier, onChangePayload: IOnChangePayload) => {
    const { fieldIdToFocus } = this.schema.onChange(fieldId, onChangePayload);
    this.setState({
      flat: this.schema.getFlatSchema(),
      tree: this.schema.getSchemaTree(),
    });
    if (onChangePayload.type === OperationTypesEnum.COLLAPSE) {
      return { fieldIdToFocus };
    }
    this.props.onChange({
      tree: this.schema.getSchemaTree(),
      flat: this.schema.getFlatSchema(),
      avroSchema: this.schema.getAvroSchema(),
    });
    if (typeof validate === 'function' && onChangePayload.value !== '') {
      validate(fieldId, this.schema.getSchemaTree());
    }
    return { fieldIdToFocus };
  };
  public render() {
    const { flat } = this.state;
    const { classes } = this.props;
    return (
      <div>
        <SchemaValidatorProvider errors={this.state.errors}>
          <div className={classes.schemaContainer}>
            <SchemaValidatorConsumer>
              {({ validate }) => {
                this.validate = validate;
                return (
                  <FieldsList
                    value={flat}
                    onChange={this.onChange.bind(this, validate)}
                    disabled={this.props.disabled}
                    visibleRowCount={this.state.schemaRowCount}
                  />
                );
              }}
            </SchemaValidatorConsumer>
          </div>
        </SchemaValidatorProvider>
      </div>
    );
  }
}

const StyledDemo = withStyles(styles)(SchemaEditorBase);
function SchemaEditor(props) {
  return (
    <ThemeWrapper>
      <StyledDemo {...props} />
    </ThemeWrapper>
  );
}

SchemaEditor.propTypes = {
  schema: PropTypes.object,
  onChange: PropTypes.func,
  disabled: PropTypes.bool,
  visibleRows: PropTypes.number,
  errors: PropTypes.object,
};

const heightOfRow = FieldsListBase.heightOfRow;

export { SchemaEditor, heightOfRow };
