import React from 'react';

import AddSchema from '../AddSchema';
import SchemaSelectorModal from '../SchemaSelectorModal';
import AlertModal from '../AlertModal';
import isEmpty from 'lodash/isEmpty';
import findIndex from 'lodash/findIndex';

require('./SchemaSelector.scss');

class SchemaSelector extends React.Component {
  schemas = [];
  constructor(props) {
    super(props);
    this.onDeleteSchema = this.onDeleteSchema.bind(this);
    this.onAlertClose = this.onAlertClose.bind(this);
    this.onAddSchemaClose = this.onAddSchemaClose.bind(this);
    this.state = {
      openSchemaModal: false,
      openAlertModal: false,
      schemaSelected: null,
      alertMessage: '',
      schemaDP: []
    }
  }

  openSchemaSelectorModal() {
    this.setState({
      schemaDP: isEmpty(this.props.availableSchemas) ? [] :
                this.props.availableSchemas.map((item) => {
                  return {
                    schemaName: item.schemaName,
                    schemaColumns: item.schemaColumns,
                    selected: false
                  }
                }),
      openSchemaModal: true
    })
  }

  onDeleteSchema(action, data) {
    if (action == 'REMOVE') {
      this.setState({
        schemaSelected: data,
        alertMessage: 'Are you sure you want to delete: ' + data.schemaName,
        openAlertModal: true
      })
    }
  }

  onAddSchemaClose(action, data) {
    if (action == 'OK') {
      if(this.isSchemaAlreadyAdded(data.schemaName)){
        alert("Schema already added");
        return;
      }
      this.props.addSelectedSchema(data);
    }
    this.setState({
      openSchemaModal: false,
    })
  }

  isSchemaAlreadyAdded(schemaName){
    return findIndex(this.props.selectedSchemas, {schemaName: schemaName}) >= 0;
  }

  onAlertClose(action) {
    console.log(this.state.schemaSelected);
    if (action === 'OK' && this.state.schemaSelected) {
      this.props.deleteSelectedSchema(this.state.schemaSelected);
    }
    this.setState({
      openAlertModal: false
    })
  }

  render() {
    return (
        <div className="schema-step-container">
          <AddSchema operation={this.openSchemaSelectorModal.bind(this)} />
          {
            this.props.selectedSchemas.map((schemaItem) => {
              return <AddSchema title={schemaItem.schemaName} data={schemaItem} key = {schemaItem.schemaName} type='ADDED'
                operation={this.onDeleteSchema} />
            })
          }
          <SchemaSelectorModal open={this.state.openSchemaModal} onClose={this.onAddSchemaClose}
            dataProvider = {this.state.schemaDP} />
          <AlertModal open={this.state.openAlertModal} message={this.state.alertMessage}
            onClose={this.onAlertClose} />
        </div>
    )
  }
}

export default SchemaSelector;