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
import {SCHEMA_TYPES, checkComplexType, getParsedSchema, checkParsedTypeForError} from 'components/SchemaEditor/SchemaHelpers';
import AbstractSchemaRow from 'components/SchemaEditor/AbstractSchemaRow';
require('./RecordSchemaRow.less');
import uuid from 'node-uuid';
import {Input} from 'reactstrap';
import SelectWithOptions from 'components/SelectWithOptions';
import {insertAt, removeAt} from 'services/helpers';
import T from 'i18n-react';
import classnames from 'classnames';

export default class RecordSchemaRow extends Component{
  constructor(props) {
    super(props);
    if (typeof props.row === 'object') {
      let displayFields = getParsedSchema(props.row).map(field => {
        field.showAbstractSchemaRow = true;
        return field;
      });
      let parsedFields = displayFields
        .map(({name, type, nullable}) => {
          return {
            name,
            type: nullable ? [type, 'null'] : type
          };
        });
      this.state = {
        type: 'record',
        name: 'a' +  uuid.v4().split('-').join(''),
        displayFields,
        error: ''
      };
      this.parsedFields = parsedFields;
    } else {
      this.state = {
        type: 'record',
        name: 'a' +  uuid.v4().split('-').join(''),
        displayFields: [
          {
            name: '',
            type: 'string',
            displayType: 'string',
            nullable: false,
            id: uuid.v4(),
            showAbstractSchemaRow: true
          }
        ],
        error: ''
      };
      this.parsedFields = [{
        name: '',
        type: 'string'
      }];
    }
    setTimeout(this.updateParent.bind(this));
  }
  onRowAdd(index) {
    let displayFields = insertAt([...this.state.displayFields], index, {
      name: '',
      displayType: 'string',
      id: uuid.v4(),
      nullable: false,
      showAbstractSchemaRow: true
    });
    let parsedFields = insertAt([...this.parsedFields], index, {
      name: '',
      type: 'string'
    });
    this.parsedFields = parsedFields;
    this.setState({ displayFields });
  }
  onRowRemove(index) {
    let displayFields = removeAt([...this.state.displayFields], index);
    let parsedFields = removeAt([...this.parsedFields], index);
    this.parsedFields = parsedFields;
    this.setState({
      displayFields,
      error: this.checkForErrors(parsedFields)
    }, this.updateParent.bind(this));
  }
  onNameChange(index, e) {
    let displayFields = this.state.displayFields;
    let parsedFields = this.parsedFields;
    displayFields[index].name = e.target.value;
    parsedFields[index].name = e.target.value;
    let error;
    error = this.checkForErrors(parsedFields);
    if (error) {
      this.setState({ error });
      return;
    }
    this.parsedFields = parsedFields;
    this.setState({
      displayFields,
      error: ''
    }, this.updateParent.bind(this));
  }
  onTypeChange(index, e) {
    let selectType = e.target.value;
    let displayFields = this.state.displayFields;
    let parsedFields = this.parsedFields;
    displayFields[index].displayType = selectType;
    displayFields[index].type = selectType;
    if (displayFields[index].nullable) {
      parsedFields[index].type = [
        selectType,
        'null'
      ];
    } else {
      parsedFields[index].type = selectType;
    }
    this.setState({
      displayFields,
      error: ''
    }, function () {
      if (!checkComplexType(selectType)) {
        this.updateParent();
      }
    }.bind(this));
  }
  onNullableChange(index, e) {
    let displayFields = this.state.displayFields;
    let parsedFields = this.parsedFields;
    displayFields[index].nullable = e.target.checked;
    if (e.target.checked) {
      parsedFields[index].type = [
        parsedFields[index].type,
        'null'
      ];
    } else {
      if (Array.isArray(parsedFields[index].type)) {
        parsedFields[index].type = parsedFields[index].type[0];
      }
    }
    this.parsedFields = parsedFields;
    this.setState({
      displayFields
    }, this.updateParent.bind(this));
  }
  checkForErrors(parsedTypes) {
    let parsedType = {
      name: this.state.name,
      type: 'record',
      fields: parsedTypes.filter(field => field.name && field.type)
    };
    return checkParsedTypeForError(parsedType);
  }
  onChildrenChange(index, fieldType) {
    let parsedFields = this.parsedFields;
    let displayFields = this.state.displayFields;

    if (displayFields[index].nullable) {
      parsedFields[index].type = [
        fieldType,
        "null"
      ];
    } else {
      parsedFields[index].type = fieldType;
    }

    let error = this.checkForErrors(parsedFields);
    if (error) {
      this.setState({error});
      return;
    }
    this.parsedFields = parsedFields;
    this.updateParent();
  }
  updateParent() {
    let error = this.checkForErrors(this.parsedFields);
    if (error) {
      this.setState({error});
      return;
    }
    this.props.onChange({
      name: this.state.name,
      type: 'record',
      fields: this.parsedFields
        .filter(field => field.name && field.type)
    });
  }
  toggleAbstractSchemaRow(index) {
    let displayFields = this.state.displayFields;
    displayFields[index].showAbstractSchemaRow = !displayFields[index].showAbstractSchemaRow;
    this.setState({
      displayFields
    });
  }
  render() {
    console.log('Inside Render of Record Schema');
    const showArrow = (row, index) => {
      if(row.showAbstractSchemaRow) {
        return (
          <span
            className="fa fa-caret-down"
            onClick={this.toggleAbstractSchemaRow.bind(this, index)}
          >
          </span>
        );
      }
      return (
        <span
          className="fa fa-caret-right"
          onClick={this.toggleAbstractSchemaRow.bind(this, index)}
        >
        </span>
      );
    };
    return (
      <div className="record-schema-row">
        <div className="text-danger">
          {this.state.error}
        </div>
        {
          this.state
              .displayFields
              .map((row, index) => {
                return (
                  <div
                    className={
                      classnames("schema-row", {
                        "nested": checkComplexType(row.displayType)
                      })
                    }
                    key={row.id}
                  >
                    <div className="field-name">
                      {
                        this.state.displayFields.length - 1 === index ?
                          <Input
                            placeholder={T.translate('features.SchemaEditor.Labels.fieldName')}
                            defaultValue={row.name}
                            onFocus={() => row.name}
                            getRef={(ref) => this.inputToFocus = ref}
                            onBlur={this.onNameChange.bind(this, index)}
                            onKeyPress={(e) => e.nativeEvent.keyCode === 13 ? this.onRowAdd(index) : null}
                          />
                        :
                          <Input
                            placeholder={T.translate('features.SchemaEditor.Labels.fieldName')}
                            defaultValue={row.name}
                            onFocus={() => row.name}
                            onBlur={this.onNameChange.bind(this, index)}
                            onKeyPress={(e) => e.nativeEvent.keyCode === 13 ? this.onRowAdd(index) : null}
                          />
                      }
                      {
                        checkComplexType(row.displayType) ?
                          showArrow(row, index)
                        :
                        null
                      }
                    </div>
                    <div className="field-type">
                      <SelectWithOptions
                        options={SCHEMA_TYPES.types}
                        value={row.displayType}
                        onChange={this.onTypeChange.bind(this, index)}
                      />
                    </div>
                    <div className="field-isnull">
                      <div className="btn btn-link">
                        <Input
                          type="checkbox"
                          checked={row.nullable}
                          onChange={this.onNullableChange.bind(this, index)}
                        />
                      </div>
                      <div className="btn btn-link">
                        <span
                          className="fa fa-plus fa-xs"
                          onClick={this.onRowAdd.bind(this, index)}
                        ></span>
                      </div>
                      <div className="btn btn-link">
                        {
                          this.state.displayFields.length !== 1 ?
                            <span
                              className="fa fa-trash fa-xs text-danger"
                              onClick={this.onRowRemove.bind(this, index)}
                              >
                            </span>
                          :
                            null
                        }
                      </div>
                    </div>
                    {
                      checkComplexType(row.displayType) && row.showAbstractSchemaRow  ?
                        <AbstractSchemaRow
                          row={{
                            type: Array.isArray(this.parsedFields[index].type) ? this.parsedFields[index].type[0] : this.parsedFields[index].type,
                            displayType: row.displayType
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

RecordSchemaRow.propTypes = {
  row: PropTypes.any,
  onChange: PropTypes.func.isRequired
};
