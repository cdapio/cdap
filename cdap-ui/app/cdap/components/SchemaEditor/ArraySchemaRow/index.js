/*
 * Copyright © 2016 Cask Data, Inc.
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

import React, {PropTypes, Component} from 'react';
  import {parseType, SCHEMA_TYPES, checkComplexType, checkParsedTypeForError} from 'components/SchemaEditor/SchemaHelpers';
import SelectWithOptions from 'components/SelectWithOptions';
import AbstractSchemaRow from 'components/SchemaEditor/AbstractSchemaRow';
import {Input} from 'reactstrap';
import classnames from 'classnames';
import cloneDeep from 'lodash/cloneDeep';

require('./ArraySchemaRow.less');

export default class ArraySchemaRow extends Component{
  constructor(props) {
    super(props);
    if (typeof props.row === 'object') {
      let item = parseType(props.row);
      let itemsType = item.type.getItemsType();
      let parsedItemsType = parseType(itemsType);
      this.state = {
        displayType: {
          type: parsedItemsType.displayType,
          nullable: parsedItemsType.nullable
        },
        showAbstractSchemaRow: true,
        error: ''
      };
      this.parsedType = itemsType;
    } else {
      this.state = {
        displayType: {
          type: 'string',
          nullable: false
        },
        showAbstractSchemaRow: true,
        error: ''
      };
      this.parsedType = 'string';
    }
    this.onTypeChange = this.onTypeChange.bind(this);
    setTimeout(this.updateParent.bind(this));
  }
  onTypeChange(e) {
    let selectedType = e.target.value;
    if (this.state.displayType.nullable) {
      this.parsedType = [selectedType, 'null'];
    } else {
      this.parsedType = selectedType;
    }
    this.setState({
      displayType: {
        type: e.target.value,
        nullable: this.state.displayType.nullable
      },
      error: ''
    }, function() {
      if (!checkComplexType(selectedType)) {
        this.updateParent();
      }
    }.bind(this));
  }
  onNullableChange(e) {
    let parsedType = cloneDeep(this.parsedType);
    if (!e.target.checked) {
      this.parsedType = parsedType[0];
    } else {
      this.parsedType = [parsedType, 'null'];
    }
    this.setState({
      displayType: {
        type: this.state.displayType.type,
        nullable: e.target.checked
      },
      error: ''
    }, this.updateParent.bind(this));
  }
  onChildrenChange(itemsState) {
    let error = checkParsedTypeForError({
      type: 'array',
      items: this.state.displayType.nullable ? [itemsState, 'null'] : itemsState
    });
    if (error) {
      this.setState({error});
      return;
    } else {
      this.setState({error: ''});
    }
    this.parsedType = this.state.displayType.nullable ? [cloneDeep(itemsState): 'null'] : cloneDeep(itemsState);
    this.updateParent();
  }
  updateParent() {
    let error = checkParsedTypeForError({
      type: 'array',
      items: this.parsedType
    });
    if (error) {
      this.setState({error});
      return;
    }
    this.props.onChange({
      type: 'array',
      items: cloneDeep(this.parsedType)
    });
  }
  toggleAbstractSchemaRow() {
    this.setState({ showAbstractSchemaRow: !this.state.showAbstractSchemaRow });
  }
  render() {
    const showArrows = () => {
      if (this.state.showAbstractSchemaRow) {
        return (
          <span
            className="fa fa-caret-down"
            onClick={this.toggleAbstractSchemaRow.bind(this)}
          >
          </span>
        );
      }
      return (
        <span
          className="fa fa-caret-right"
          onClick={this.toggleAbstractSchemaRow.bind(this)}
        >
        </span>
      );
    };
    return (
      <div className="array-schema-row">
        <div className="text-danger">
          {this.state.error}
        </div>
        <div className={
            classnames("schema-row", {
              "nested": checkComplexType(this.state.displayType.type)
            })
          }>
          <div className="field-name">
            <SelectWithOptions
              options={SCHEMA_TYPES.types}
              value={this.state.displayType.type}
              onChange={this.onTypeChange}
            />
          {
            checkComplexType(this.state.displayType.type) ?
              showArrows()
            :
              null
          }
          </div>
          <div className="field-type"></div>
          <div className="field-isnull">
            <div className="btn btn-link">
              <Input
                type="checkbox"
                checked={this.state.displayType.nullable}
                onChange={this.onNullableChange.bind(this)}
              />
            </div>
          </div>
          {
            checkComplexType(this.state.displayType.type) && this.state.showAbstractSchemaRow  ?
              <AbstractSchemaRow
                row={{
                  type: Array.isArray(this.parsedType) ? this.parsedType[0] : this.parsedType,
                  displayType: this.state.displayType.type
                }}
                onChange={this.onChildrenChange.bind(this)}
              />
            :
              null
          }
        </div>
      </div>
    );
  }
}

ArraySchemaRow.propTypes = {
  row: PropTypes.any,
  onChange: PropTypes.func.isRequired
};
