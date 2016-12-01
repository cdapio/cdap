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
import SelectWithOptions from 'components/SelectWithOptions';
import {parseType, SCHEMA_TYPES, checkComplexType, checkParsedTypeForError} from 'components/SchemaEditor/SchemaHelpers';
import AbstractSchemaRow from 'components/SchemaEditor/AbstractSchemaRow';
import {Input} from 'reactstrap';
import classnames from 'classnames';
import cloneDeep from 'lodash/cloneDeep';

require('./MapSchemaRow.less');

export default class MapSchemaRow extends Component {
  constructor(props) {
    super(props);
    if (typeof props.row === 'object') {
      let rowType = parseType(props.row);
      let parsedKeysType = parseType(rowType.type.getKeysType());
      let parsedValuesType = parseType(rowType.type.getValuesType());
      this.state = {
        name: rowType.name,
        keysType: parsedKeysType.displayType,
        keysTypeNullable: parsedKeysType.nullable,
        valuesType: parsedValuesType.displayType,
        valuesTypeNullable: parsedValuesType.nullable,
        error: '',
        showKeysAbstractSchemaRow: true,
        showValuesAbstractSchemaRow: true
      };
      this.parsedKeysType = rowType.type.getKeysType();
      this.parsedValuesType = rowType.type.getValuesType();
    } else {
      this.state = {
        name: props.row.name,
        keysType: 'string',
        keysTypeNullable: false,
        valuesType: 'string',
        valuesTypeNullable: false,
        error: '',
        showKeysAbstractSchemaRow: true,
        showValuesAbstractSchemaRow: true
      };
      this.parsedKeysType = 'string';
      this.parsedValuesType = 'string';
    }
    setTimeout(this.updateParent.bind(this));
    this.onKeysTypeChange = this.onKeysTypeChange.bind(this);
    this.onValuesTypeChange = this.onValuesTypeChange.bind(this);
  }
  onKeysTypeChange(e) {
    this.parsedKeysType = this.state.keysTypeNullable ? [e.target.value, 'null'] : e.target.value;
    this.setState({
      keysType: e.target.value
    }, function() {
      if (!checkComplexType(this.state.keysType)) {
        this.updateParent();
      }
    }.bind(this));
  }
  onValuesTypeChange(e) {
    this.parsedValuesType = this.state.valuesTypeNullable ? [e.target.value, 'null'] : e.target.value;
    this.setState({
      valuesType: e.target.value
    }, function() {
      if (!checkComplexType(this.state.valuesType)) {
        this.updateParent();
      }
    }.bind(this));
  }
  updateParent() {
    let error = checkParsedTypeForError(this.parsedValuesType) || checkParsedTypeForError(this.parsedKeysType);
    if (error) {
      this.setState({error});
      return;
    }
    this.props.onChange({
      type: 'map',
      keys: this.parsedKeysType,
      values: this.parsedValuesType
    });
  }
  onKeysChildrenChange(keysState) {
    keysState = cloneDeep(keysState);
    let error = checkParsedTypeForError(keysState);
    if (error) {
      this.setState({error});
      return;
    }
    this.parsedKeysType = this.state.keysTypeNullable ? [keysState, 'null']: keysState;
    this.updateParent();
  }
  onValuesChildrenChange(valuesState) {
    valuesState = cloneDeep(valuesState);
    let error = checkParsedTypeForError(valuesState);
    if (error) {
      this.setState({error});
      return;
    }
    this.parsedValuesType = this.state.valuesTypeNullable ? [valuesState, 'null']: valuesState;
    this.updateParent();
  }
  onKeysTypeNullableChange(e) {
    if (e.target.checked) {
      this.parsedKeysType = [this.parsedKeysType, 'null'];
    } else {
      this.parsedKeysType = this.parsedKeysType[0];
    }
    this.setState({
      keysTypeNullable: e.target.checked,
      error: ''
    }, this.updateParent.bind(this));
  }
  onValuesTypeNullableChange(e) {
    if (e.target.checked) {
      this.parsedValuesType = [this.parsedValuesType, 'null'];
    } else {
      this.parsedValuesType = this.parsedValuesType[0];
    }
    this.setState({
      valuesTypeNullable: e.target.checked,
      error: ''
    }, this.updateParent.bind(this));
  }
  toggleKeysAbstractSchemaRow() {
    this.setState({showKeysAbstractSchemaRow: !this.state.showKeysAbstractSchemaRow});
  }
  toggleValuesAbstractSchemaRow() {
    this.setState({showValuesAbstractSchemaRow: !this.state.showValuesAbstractSchemaRow});
  }
  render() {
    const showKeysArrow = () => {
      if (this.state.showKeysAbstractSchemaRow) {
        return (
          <span
            className="fa fa-caret-down"
            onClick={this.toggleKeysAbstractSchemaRow.bind(this)}
          >
          </span>
        );
      }
      return (
        <span
          className="fa fa-caret-right"
          onClick={this.toggleKeysAbstractSchemaRow.bind(this)}
        >
        </span>
      );
    };
    const showValuesArrow = () => {
      if (this.state.showValuesAbstractSchemaRow) {
        return (
          <span
            className="fa fa-caret-down"
            onClick={this.toggleValuesAbstractSchemaRow.bind(this)}
          >
          </span>
        );
      }
      return (
        <span
          className="fa fa-caret-right"
          onClick={this.toggleValuesAbstractSchemaRow.bind(this)}
        >
        </span>
      );
    };
    return (
      <div className="map-schema-row">
        <div className="text-danger">
          {this.state.error}
        </div>
        <div className="schema-row">
          <div className={
            classnames("key-row clearfix", {
              "nested": checkComplexType(this.state.keysType)
            })
          }>
            <div className="field-name">
              <div> Key: </div>
              <SelectWithOptions
                options={SCHEMA_TYPES.types}
                value={this.state.keysType}
                onChange={this.onKeysTypeChange}
              />
              {
                checkComplexType(this.state.keysType) ?
                  showKeysArrow()
                :
                null
              }
            </div>
            <div className="field-type"></div>
            <div className="field-isnull">
              <div className="btn btn-link">
                <Input
                  type="checkbox"
                  checked={this.state.keysTypeNullable}
                  onChange={this.onKeysTypeNullableChange.bind(this)}
                />
              </div>
            </div>
            {
              checkComplexType(this.state.keysType) && this.state.showKeysAbstractSchemaRow ?
                <AbstractSchemaRow
                  row={{
                    type: this.parsedKeysType,
                    displayType: this.state.keysType
                  }}
                  onChange={this.onKeysChildrenChange.bind(this)}
                />
              :
                null
            }
          </div>
          <div className={
            classnames("value-row clearfix", {
              "nested": checkComplexType(this.state.valuesType)
            })
          }>
            <div className="field-name">
              <div>Value: </div>
              <SelectWithOptions
                options={SCHEMA_TYPES.types}
                value={this.state.valuesType}
                onChange={this.onValuesTypeChange}
              />
              {
                checkComplexType(this.state.valuesType)?
                  showValuesArrow()
                :
                null
              }
            </div>
            <div className="field-type"></div>
            <div className="field-isnull">
              <div className="btn btn-link">
                <Input
                  type="checkbox"
                  checked={this.state.valuesTypeNullable}
                  onChange={this.onValuesTypeNullableChange.bind(this)}
                />
              </div>
            </div>
              {
                checkComplexType(this.state.valuesType) && this.state.showValuesAbstractSchemaRow ?
                  <AbstractSchemaRow
                    row={{
                      type: Array.isArray(this.parsedValuesType) ? this.parsedValuesType[0] : this.parsedValuesType,
                      displayType: this.state.valuesType
                    }}
                    onChange={this.onValuesChildrenChange.bind(this)}
                  />
                :
                  null
              }
          </div>
        </div>
      </div>
    );
  }
}

MapSchemaRow.propTypes = {
  row: PropTypes.any,
  onChange: PropTypes.func.isRequired
};
