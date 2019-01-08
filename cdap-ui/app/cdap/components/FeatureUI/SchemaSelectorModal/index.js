/* eslint react/prop-types: 0 */
import React from 'react';
import { Button, Modal, ModalHeader, ModalBody, ModalFooter } from 'reactstrap';
import isEmpty from 'lodash/isEmpty';
import cloneDeep from 'lodash/cloneDeep';


import { ListGroup, ListGroupItem } from 'reactstrap';
import CheckList from '../CheckList';
import { toCamelCase } from '../util';

require('./SchemaSelectorModal.scss');

class SchemaSelectorModal extends React.Component {
  changedColumnList;
  constructor(props) {
    super(props);
    this.state = {
      open: this.props.open,
      showSchemaSelector: true,
      selectedSchema: undefined,
      allSelected: false,
      columns: [],
      operationType: 'ADD'
    };

    this.onCancel = this.onCancel.bind(this);
    this.onDone = this.onDone.bind(this);
    this.onSchemaClick = this.onSchemaClick.bind(this);
  }

  componentWillReceiveProps(props)  {
    let columns = isEmpty(props.selectedSchema) ? [] : props.selectedSchema.schemaColumns.map(column => {
      return { name: column.columnName, description: toCamelCase(column.columnType), checked: column.checked };
    });

    this.setState({
      selectedSchema: props.selectedSchema,
      showSchemaSelector: props.showSchemaSelector,
      columns: columns,
      operationType: props.operationType,
      allSelected: columns.length && this.getCheckedCount(columns) == columns.length
    });
  }

  onCancel() {
    this.props.onClose('CANCEL', {});
  }

  onDone() {
    let finalSchema = cloneDeep(this.state.selectedSchema);
    finalSchema.schemaColumns = finalSchema.schemaColumns.filter((item, index) => this.changedColumnList.get(index));
    this.props.onClose('OK', finalSchema, this.state.operationType);
  }

  onSchemaClick(schema) {
    if (this.lastSelectedSchema) {
      this.lastSelectedSchema.selected = false;
    }
    schema.selected = true;
    this.lastSelectedSchema = schema;
    let columns = isEmpty(schema) ? [] : schema.schemaColumns.map(column => {
      return { name: column.columnName, description: toCamelCase(column.columnType), checked: false };
    });
    this.changedColumnList = new Map();
    this.setState({
      selectedSchema: schema,
      columns: columns,
      allSelected: false
    });
  }

  handleColumnChange(changeList) {
    this.changedColumnList = changeList;
    let checkedCount = 0;
    let columns = this.state.columns.map((column, index) => {
      column.checked = changeList.get(index);
      column.checked && checkedCount++;
      return column;
    });
    this.setState({
      allSelected: checkedCount == columns.length,
      columns: columns
    });
  }

  getCheckedCount(columns) {
    let checkedCount = 0;
    if (!isEmpty(columns)) {
      for (let index = 0; index < columns.length; index++) {
        columns[index].checked && checkedCount++;
      }
    }
    return checkedCount;
  }

  onSelectAll(event) {
    const isChecked = event.target.checked;
    this.changedColumnList  = new Map();
    this.setState(prevState => ({
      allSelected: isChecked,
      columns: prevState.columns.map((column, index) => {
        column.checked = isChecked;
        column.checked && this.changedColumnList.set(index, column.checked);
        return column;
      }),
    }));
  }

  render() {
    return (
      <div>
        <Modal isOpen={this.props.open}
          zIndex='1070'>
          <ModalHeader>Select Columns</ModalHeader>
          <ModalBody>
            <div className='body-container'>
              {
                this.state.showSchemaSelector &&
                <div className='schema-container'>
                  <div className='schema-header'>Schema</div>
                  <ListGroup>
                    {
                      this.props.dataProvider.map((item) => {
                        return (<ListGroupItem active={item.selected} key={item.schemaName}
                          onClick={() => this.onSchemaClick(item)}>{item.schemaName}</ListGroupItem>);
                      })
                    }
                  </ListGroup>
                </div>
              }
              <div className='column-container'>
                <div className='schema-header'>{"Select Columns: " + (this.state.selectedSchema ? this.state.selectedSchema.schemaName : "")}</div>
                {
                  !isEmpty(this.state.columns) &&
                  <div className='column-control'>
                    <label className='select-all-container'>
                      <input type="checkbox" checked={this.state.allSelected} onClick={this.onSelectAll.bind(this)} />
                      Select All
                    </label>
                    <div className='column-header'>
                      <div className='column-name'>Column Name</div>
                      <div className='column-type'>Type</div>
                    </div>
                  </div>
                }
                <div className='column-checklist-container'>
                  <CheckList className="column-list" dataProvider={this.state.columns} handleChange={this.handleColumnChange.bind(this)} />
                </div>
              </div>
            </div>
          </ModalBody>
          <ModalFooter>
            <Button className="btn-margin" color="secondary" onClick={this.onCancel}>Cancel</Button>
            <Button className="btn-margin" color="primary" onClick={this.onDone}
              disabled={this.getCheckedCount(this.state.columns) < 1} >Done</Button>{' '}
          </ModalFooter>
        </Modal>
      </div>
    );
  }
}

export default SchemaSelectorModal;
