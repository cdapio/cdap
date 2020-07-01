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
import {
  IFlattenRowType,
  IFieldIdentifier,
  IOnChangePayload,
  IRowOnChangeHandler,
} from 'components/AbstractWidget/SchemaEditor/EditorTypes';
import {
  schemaTypes,
  InternalTypesEnum,
  OperationTypesEnum,
} from 'components/AbstractWidget/SchemaEditor/SchemaConstants';
import { FieldType } from 'components/AbstractWidget/SchemaEditor/FieldType';
import { UnionType } from 'components/AbstractWidget/SchemaEditor/UnionType';
import { MapType } from 'components/AbstractWidget/SchemaEditor/MapType';
import { EnumType } from 'components/AbstractWidget/SchemaEditor/EnumType';
import { ArrayType } from 'components/AbstractWidget/SchemaEditor/ArrayType';
import { FieldWrapper } from 'components/AbstractWidget/SchemaEditor/FieldWrapper';
import { SchemaValidatorConsumer } from 'components/AbstractWidget/SchemaEditor/SchemaValidator';
import If from 'components/If';
import ErrorIcon from '@material-ui/icons/ErrorOutline';
import classnames from 'classnames';
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import Tooltip from '@material-ui/core/Tooltip';
import KeyboardArrowDownIcon from '@material-ui/icons/KeyboardArrowDown';
import KeyboardArrowRightIcon from '@material-ui/icons/KeyboardArrowRight';

const styles = (theme): StyleRules => {
  return {
    errorIcon: {
      position: 'absolute',
      right: '5px',
      color: theme.palette.red[200],
    },
    erroredRow: {
      outline: `2px solid ${theme.palette.red[200]}`,
    },
    tooltip: {
      backgroundColor: theme.palette.red[200],
      color: 'white',
      fontSize: '12px',
      wordBreak: 'break-word',
    },
  };
};

interface IFieldRowState {
  name: string;
  type: string;
  nullable: boolean;
  typeProperties: Record<string, string>;
}

interface IFieldRowProps extends WithStyles<typeof styles> {
  field: IFlattenRowType;
  onChange: IRowOnChangeHandler;
  autoFocus?: boolean;
}

class FieldRowBase extends React.Component<IFieldRowProps, IFieldRowState> {
  public state: IFieldRowState = {
    name: '',
    type: schemaTypes[0],
    nullable: false,
    typeProperties: {},
  };

  constructor(props) {
    super(props);
    const { field } = this.props;
    this.state = {
      name: field.name,
      type: field.type,
      nullable: field.nullable,
      typeProperties: field.typeProperties,
    };
  }

  public componentWillReceiveProps() {
    return;
  }

  public onChange = (property: string, value) => {
    if (['name', 'type', 'nullable', 'typeProperties'].indexOf(property) === -1) {
      return;
    }
    const { onChange, field } = this.props;
    if (onChange) {
      this.props.onChange(
        { id: field.id, ancestors: field.ancestors },
        {
          property,
          value,
          type: OperationTypesEnum.UPDATE,
        }
      );
    }
    return;
  };

  public onAdd = () => {
    const { onChange, field } = this.props;
    const { id, ancestors } = field;
    if (onChange) {
      this.props.onChange(
        { id, ancestors },
        {
          type: OperationTypesEnum.ADD,
        }
      );
    }
  };

  public onRemove = () => {
    const { onChange, field } = this.props;
    const { id, ancestors } = field;
    if (onChange) {
      onChange({ id, ancestors }, { type: OperationTypesEnum.REMOVE });
    }
  };

  public onToggleCollapse = () => {
    const { onChange, field } = this.props;
    const { id, ancestors } = field;
    if (onChange) {
      onChange({ id, ancestors }, { type: OperationTypesEnum.COLLAPSE });
    }
  };

  public RenderSubType = (field) => {
    switch (field.internalType) {
      case InternalTypesEnum.RECORD_SIMPLE_TYPE:
      case InternalTypesEnum.RECORD_COMPLEX_TYPE_ROOT:
        return (
          <FieldType
            name={this.props.field.name}
            type={this.props.field.type}
            nullable={this.props.field.nullable}
            onChange={this.onChange}
            onAdd={this.onAdd}
            onRemove={this.onRemove}
            autoFocus={this.props.autoFocus}
            typeProperties={this.props.field.typeProperties}
          />
        );
      case InternalTypesEnum.ARRAY_SIMPLE_TYPE:
      case InternalTypesEnum.ARRAY_COMPLEX_TYPE:
      case InternalTypesEnum.ARRAY_COMPLEX_TYPE_ROOT:
        return (
          <ArrayType
            type={this.props.field.type}
            nullable={this.props.field.nullable}
            onChange={this.onChange}
            onAdd={this.onAdd}
            onRemove={this.onRemove}
            autoFocus={this.props.autoFocus}
            typeProperties={this.props.field.typeProperties}
          />
        );
      case InternalTypesEnum.ENUM_SYMBOL:
        return (
          <EnumType
            typeProperties={this.props.field.typeProperties}
            onChange={this.onChange}
            onAdd={this.onAdd}
            onRemove={this.onRemove}
            autoFocus={this.props.autoFocus}
          />
        );
      case InternalTypesEnum.MAP_KEYS_COMPLEX_TYPE_ROOT:
      case InternalTypesEnum.MAP_KEYS_SIMPLE_TYPE:
      case InternalTypesEnum.MAP_VALUES_COMPLEX_TYPE_ROOT:
      case InternalTypesEnum.MAP_VALUES_SIMPLE_TYPE:
        return (
          <MapType
            internalType={this.props.field.internalType}
            type={this.props.field.type}
            nullable={this.props.field.nullable}
            onChange={this.onChange}
            onAdd={this.onAdd}
            onRemove={this.onRemove}
            autoFocus={this.props.autoFocus}
            typeProperties={this.props.field.typeProperties}
          />
        );
      case InternalTypesEnum.UNION_SIMPLE_TYPE:
      case InternalTypesEnum.UNION_COMPLEX_TYPE_ROOT:
        return (
          <UnionType
            type={this.props.field.type}
            nullable={this.props.field.nullable}
            onChange={this.onChange}
            onAdd={this.onAdd}
            onRemove={this.onRemove}
            autoFocus={this.props.autoFocus}
            typeProperties={this.props.field.typeProperties}
          />
        );
      default:
        return null;
    }
  };

  public render() {
    const { classes } = this.props;
    const { ancestors, internalType } = this.props.field;
    if (internalType === InternalTypesEnum.SCHEMA) {
      return null;
    }
    return (
      <SchemaValidatorConsumer>
        {({ errorMap = {} }) => {
          const hasError = errorMap.hasOwnProperty(this.props.field.id);
          return (
            <FieldWrapper
              ancestors={ancestors}
              className={classnames({
                [classes.erroredRow]: hasError,
              })}
            >
              <React.Fragment>
                <If condition={hasError}>
                  <Tooltip
                    classes={{ tooltip: classes.tooltip }}
                    title={errorMap[this.props.field.id]}
                    placement="right"
                  >
                    <ErrorIcon className={classes.errorIcon} />
                  </Tooltip>
                </If>
                <If condition={typeof this.props.field.collapsed === 'boolean'} invisible>
                  <If condition={this.props.field.collapsed}>
                    <KeyboardArrowRightIcon onClick={this.onToggleCollapse} />
                  </If>
                  <If condition={!this.props.field.collapsed}>
                    <KeyboardArrowDownIcon onClick={this.onToggleCollapse} />
                  </If>
                </If>
                {this.RenderSubType(this.props.field)}
              </React.Fragment>
            </FieldWrapper>
          );
        }}
      </SchemaValidatorConsumer>
    );
  }
}

const FieldRow = React.memo(withStyles(styles)(FieldRowBase));
export { FieldRow };
