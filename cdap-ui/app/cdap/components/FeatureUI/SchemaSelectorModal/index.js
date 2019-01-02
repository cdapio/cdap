
import React from 'react';
import { Button, Modal, ModalHeader, ModalBody, ModalFooter } from 'reactstrap';
import isEmpty from 'lodash/isEmpty'
import cloneDeep from 'lodash/cloneDeep'


import { ListGroup, ListGroupItem } from 'reactstrap';
import CheckList from '../CheckList';

require('./SchemaSelectorModal.scss');

class SchemaSelectorModal extends React.Component {
  changedColumnList;
  constructor(props) {
    super(props);
    this.state = {
      open: this.props.open,
      selectedSchema: undefined,
    }

    this.onCancel = this.onCancel.bind(this);
    this.onDone = this.onDone.bind(this);
    this.onSchemaClick = this.onSchemaClick.bind(this);
  }

  onCancel() {
    this.props.onClose('CANCEL', {});
    this.state.selectedSchema = undefined;
  }

  onDone() {
    let finalSchema = cloneDeep(this.state.selectedSchema);
    finalSchema.schemaColumns = finalSchema.schemaColumns.filter((item,index) => this.changedColumnList.get(index));
    this.props.onClose('OK', finalSchema);
    this.state.selectedSchema = undefined;
  }

  onSchemaClick(schema) {
    if (this.lastSelectedSchema) {
      this.lastSelectedSchema.selected = false;
    }
    schema.selected = true;
    this.lastSelectedSchema = schema;
    this.setState({
      selectedSchema: schema
    })
  }

  handleColumnChange(changeList){
    this.changedColumnList = changeList;
  }

  render() {
    let columns = isEmpty(this.state.selectedSchema) ? [] : this.state.selectedSchema.schemaColumns.map(column => {
      return { name: column.columnName, description: column.columnType, checked: false }
    });
    this.changedColumnList = new Map;
    return (
      <div>
        <Modal isOpen={this.props.open}
          zIndex='1070'
          onRequestClose={this.onCancel}>
          <ModalBody>
            <div className='body-container'>
              <div className='schema-container'>
                <ListGroup>Schema
                  {
                    this.props.dataProvider.map((item) => {
                      return <ListGroupItem active={item.selected} onClick={() => this.onSchemaClick(item)}>{item.schemaName}</ListGroupItem>
                    })
                  }
                </ListGroup>
              </div>
              <div className='column-container'>
                <CheckList dataProvider = {columns} title="Select Columns" handleChange = {this.handleColumnChange.bind(this)}/>
              </div>
            </div>
          </ModalBody>
          <ModalFooter>
            <Button className = "btn-margin" color="secondary" onClick={this.onCancel}>Cancel</Button>
            <Button className = "btn-margin" color="primary" onClick={this.onDone}>Done</Button>{' '}
          </ModalFooter>
        </Modal>
      </div>
    );
  }
}

export default SchemaSelectorModal;