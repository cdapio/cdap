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
import {insertAt, removeAt} from 'services/helpers';
import uuid from 'node-uuid';
import classnames from 'classnames';
import cloneDeep from 'lodash/cloneDeep';

require('./UnionSchemaRow.less');

export default class UnionSchemaRow extends Component {
  constructor(props) {
    super(props);
    if (typeof props.row === 'object') {
      let rowType = parseType(props.row);
      let parsedTypes = rowType.type.getTypes();
      let displayTypes = parsedTypes.map(type => Object.assign({}, parseType(type), {id: 'a' + uuid.v4()}));
      this.state = {
        displayTypes,
        error: ''
      };
      this.parsedTypes = parsedTypes;
    } else {
      this.state = {
        displayTypes: [
          {
            displayType: 'string',
            type: 'string',
            id: uuid.v4(),
            nullable: false
          }
        ],
        error: ''
      };
      this.parsedTypes = [
        'string'
      ];
    }
    setTimeout(this.updateParent.bind(this));
    this.onTypeChange = this.onTypeChange.bind(this);
  }
  updateParent() {
    let error = checkParsedTypeForError(this.parsedTypes);
    if (error) {
      this.setState({error});
      return;
    }
    this.props.onChange(this.parsedTypes);
  }
  onNullableChange(index, e) {
    let displayTypes = this.state.displayTypes;
    let parsedTypes = cloneDeep(this.parsedTypes);
    displayTypes[index].nullable = e.target.checked;
    if (e.target.checked) {
      parsedTypes[index] = [
        parsedTypes[index],
        'null'
      ];
    } else {
      parsedTypes[index] = parsedTypes[index][0];
    }
    this.parsedTypes = parsedTypes;
    this.setState({
      displayTypes,
      error: ''
    }, this.updateParent.bind(this));
  }
  onTypeChange(index, e) {
    let selectedType = e.target.value;
    let displayTypes = this.state.displayTypes;
    displayTypes[index].displayType = selectedType;
    displayTypes[index].type = selectedType;
    let parsedTypes = cloneDeep(this.parsedTypes);
    if (displayTypes[index].nullable) {
      parsedTypes[index] = [
        selectedType,
        'null'
      ];
    } else {
      parsedTypes[index] = selectedType;
    }
    this.parsedTypes = parsedTypes;
    this.setState({
      displayTypes,
      error: ''
    }, () => {
      if (!checkComplexType(selectedType)) {
        this.updateParent();
      }
    });
  }
  onTypeAdd(index) {
    let displayTypes = insertAt([...this.state.displayTypes], index, {
      type: 'string',
      displayType: 'string',
      id: uuid.v4(),
      nullable: false
    });
    let parsedTypes = insertAt(cloneDeep(this.parsedTypes), index, 'string');
    this.parsedTypes = parsedTypes;
    this.setState({ displayTypes }, this.updateParent.bind(this));
  }
  onTypeRemove(index) {
    let displayTypes = removeAt([...this.state.displayTypes], index);
    let parsedTypes = removeAt(cloneDeep(this.parsedTypes), index);
    this.parsedTypes = parsedTypes;
    this.setState({ displayTypes }, this.updateParent.bind(this));
  }
  onChildrenChange(index, parsedType) {
    let parsedTypes = cloneDeep(this.parsedTypes);
    let displayTypes = this.state.displayTypes;
    let error;
    if (displayTypes[index].nullable) {
      parsedTypes[index] = [
        parsedType,
        "null"
      ];
    } else {
      parsedTypes[index] = parsedType;
    }
    error = checkParsedTypeForError(parsedTypes);
    if (error) {
      this.setState({error});
      return;
    }
    this.parsedTypes = parsedTypes;
    this.updateParent();
  }
  render() {
    return (
      <div className="union-schema-row">
        <div className="text-danger">
          {this.state.error}
        </div>
          {
            this.state.displayTypes.map((displayType, index) => {
              return (
                <div
                  className={
                    classnames("schema-row", {
                      "nested": checkComplexType(displayType.displayType)
                    })
                  }
                  key={displayType.id}
                >
                  <div className="field-name">
                    <SelectWithOptions
                      options={SCHEMA_TYPES.types}
                      value={displayType.displayType}
                      onChange={this.onTypeChange.bind(this, index)}
                    />
                  </div>
                  <div className="field-type"></div>
                  <div className="field-isnull">
                    <div className="btn btn-link">
                      <Input
                        type="checkbox"
                        checked={displayType.nullable}
                        onChange={this.onNullableChange.bind(this, index)}
                      />
                    </div>
                    <div className="btn btn-link">
                      <span
                        className="fa fa-plus"
                        onClick={this.onTypeAdd.bind(this, index)}
                      ></span>
                    </div>
                    <div className="btn btn-link">
                      {
                        this.state.displayTypes.length !== 1 ?
                          <span
                            className="fa fa-trash fa-xs text-danger"
                            onClick={this.onTypeRemove.bind(this, index)}
                          >
                          </span>
                        :
                          null
                      }
                    </div>
                  </div>
                  {
                    checkComplexType(displayType.displayType) ?
                      <AbstractSchemaRow
                        row={{
                          type: Array.isArray(displayType.type) ? displayType.type[0] : displayType.type,
                          displayType: displayType.displayType
                        }}
                        onChange={this.onChildrenChange.bind(this, index)}
                      />
                    :
                      null
                  }
                </div>
              );
            })
          }
      </div>
    );
  }
}
UnionSchemaRow.propTypes = {
  row: PropTypes.any,
  onChange: PropTypes.func.isRequired
};
