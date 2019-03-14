/*
 * Copyright © 2017 Cask Data, Inc.
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

/* eslint react/prop-types: 0 */
import React from 'react';
import { Button, Modal, ModalHeader, ModalBody, ModalFooter } from 'reactstrap';
import isEmpty from 'lodash/isEmpty';


import { ListGroup, ListGroupItem } from 'reactstrap';
import { toCamelCase } from '../util';

require('./SchemaSelectorModal.scss');

class SchemaSelectorModal extends React.Component {
  checkedList;
  constructor(props) {
    super(props);
    this.checkedList = new Set();
    this.state = {
      open: this.props.open,
      currentSchema: undefined,
      columns: [],
      operationType: 'ADD',
    };

    this.onCancel = this.onCancel.bind(this);
    this.onDone = this.onDone.bind(this);
    this.onSchemaClick = this.onSchemaClick.bind(this);
  }

  componentWillReceiveProps(props) {
    this.checkedList = new Set();
    props.selectedSchemas.forEach(schema => {
      this.checkedList.add(schema);
    });
    this.setState({
      columns: [],
      currentSchema: undefined,
      operationType: props.operationType,
    });
  }

  onCancel() {
    this.props.onClose('CANCEL', {});
  }

  onDone() {
    this.props.onClose('OK', Array.from(this.checkedList), this.state.operationType);
  }

  onSchemaClick(schema) {
    if (this.lastSelectedSchema) {
      this.lastSelectedSchema.selected = false;
    }
    schema.selected = true;
    this.lastSelectedSchema = schema;
    let columns = isEmpty(schema) ? [] : schema.schemaColumns.map(column => {
      return { name: column.columnName, description: toCamelCase(column.columnType)};
    });
    this.setState({
      currentSchema: schema,
      columns: columns,
    });
  }

  onSchemaChecked(item, event) {
    const isChecked = event.target.checked;
    if(isChecked) {
      this.checkedList.add(item.schemaName);
    } else {
      this.checkedList.delete(item.schemaName);
    }
  }

  render() {
    return (
      <div>
        <Modal isOpen={this.props.open}
          zIndex='1070'>
          <ModalHeader>Select Schemas</ModalHeader>
          <ModalBody>
            <div className='body-container'>
              {
                <div className='schema-container'>
                  <div className='schema-header'>Schema</div>
                  <ListGroup>
                    {
                      this.props.dataProvider.map((item) => {
                        return (<ListGroupItem active={item.selected} key={item.schemaName}
                          onClick={() => this.onSchemaClick(item)}>
                          <input type="checkbox"
                              checked = { this.checkedList.has(item.schemaName) || false}
                              onChange= { this.onSchemaChecked.bind(this, item)} />
                          <div className='check-box-label'>{item.schemaName}</div>
                          {
                            item.selected &&  <i className="fa fa-caret-right select-icon"></i>
                          }
                        </ListGroupItem>);
                      })
                    }
                  </ListGroup>
                </div>
              }
              <div className='column-container'>
                <div className='schema-header'>{"Columns of Schema: " + (this.state.currentSchema ? this.state.currentSchema.schemaName : "")}</div>
                {
                  !isEmpty(this.state.columns) &&
                  <div className='column-header'>
                    <div className='column-name'>Column Name</div>
                    <div className='column-type'>Type</div>
                  </div>
                }
                <div className='column-list-container'>
                  <div className='list'>
                    {
                      isEmpty(this.state.columns) ? 'No Data' : (
                        this.state.columns.map(item => {
                          return (
                            <div className='list-item' key={item.name}>
                              <div className='item-name'>{item.name}</div>
                              <div className='item-property'>{item.description}</div>
                            </div>);
                        })
                      )}
                  </div>
                </div>
              </div>
            </div>
          </ModalBody>
          <ModalFooter>
            <Button className="btn-margin" color="secondary" onClick={this.onCancel}>Cancel</Button>
            <Button className="btn-margin" color="primary" onClick={this.onDone}
              disabled={this.checkedList.size < 1} >Done</Button>{' '}
          </ModalFooter>
        </Modal>
      </div>
    );
  }
}

export default SchemaSelectorModal;
